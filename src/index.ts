import path from 'path';
import Database, { Statement } from 'better-sqlite3';
import fs from 'fs';
import { Connection, PublicKey, ConfirmedTransaction, Transaction, ConfirmedSignatureInfo } from '@solana/web3.js';
import { MangoInstructionLayout, sleep, IDS, MangoClient} from '@blockworks-foundation/mango-client';
const schema_1 = require("@blockworks-foundation/mango-client/lib/schema.js");

var db; 
var vaultSymbolMap;
var oracleSymbolMap;
var mangoGroupSymbolMap;
var symbolMintDecimalsMap;
const requestWaitTime = 500;

const oracleProgramId = 'FjJ5mhdRWeuaaremiHjeQextaAw1sKWDqr3D7pXjgztv';
const mangoProgramId = 'JD3bq9hGdy38PuWQ4h2YJpELmHVGPPfFSuFkpzAd9zfu';

async function getNewSignatures(afterSignature: string, connection: Connection, account: PublicKey) {
    // Fetches all signatures associated with the account - working backwards in time until it encounters the "afterSignature" signature

    let signatures;
    const limit = 1000;
    let before = null;
    let options;
    let allSignaturesInfo: ConfirmedSignatureInfo[] = [];
    while (true) {

        if (before === null) {
            options = {limit: limit};
        } else {
            options = {limit: limit, before: before};
        }
        
        let signaturesInfo = (await connection.getConfirmedSignaturesForAddress2(account, options));
        signatures = signaturesInfo.map(x => x['signature']);

        let latestDbSignatureIndex = signatures.indexOf(afterSignature);

        if (latestDbSignatureIndex !== -1) {
            allSignaturesInfo = allSignaturesInfo.concat(signaturesInfo.slice(0, latestDbSignatureIndex));
            break
        } else {
            allSignaturesInfo = allSignaturesInfo.concat(signaturesInfo);
            
        }
        before = signatures[signatures.length-1];

        console.log(new Date(signaturesInfo[signaturesInfo.length-1].blockTime! * 1000).toISOString());

        await sleep(requestWaitTime);
    }

    return allSignaturesInfo
}


async function getLatestSignatureBeforeUnixEpoch(connection, account, unixEpoch) {

    let signaturesInfo;  
    let limit = 1000;
    let options;
    let earliestSignature = null;
    let signature;
    let found = false;
    while (true) {
      
        if (earliestSignature === null) {
            options = {limit: limit};
        } else {
            options = {limit: limit, before: earliestSignature};
        }
        
        signaturesInfo = await connection.getConfirmedSignaturesForAddress2(account, options);

        for (let signatureInfo of signaturesInfo) {
            if (signatureInfo.blockTime < unixEpoch) {
                signature = signatureInfo.signature;
                found = true;
                break
            }
        }
        if (found) {break}

        earliestSignature = signaturesInfo[signaturesInfo.length - 1].signature;

        console.log(new Date(signaturesInfo[signaturesInfo.length-1].blockTime! * 1000).toISOString());

        await sleep(requestWaitTime);
    }

    return signature;
}


async function insertNewSignatures(account, connection) {
    let latestDbSignatureRow = db.prepare('select signature from transactions where id = (select max(id) from transactions where account = ?)').get(account.toBase58());

    let latestDbSignature;
    if (!latestDbSignatureRow) {

        let currentUnixEpoch = Math.round(Date.now()/1000);
        // If tranasctions table is empty - initialise by getting all signatures in the 6 hours
        let latestSignatureUnixEpoch = currentUnixEpoch - 6 * 60 * 60;

        console.log('Current time ', new Date(currentUnixEpoch * 1000).toISOString());
        console.log('Getting all signatures after ', new Date(latestSignatureUnixEpoch * 1000).toISOString());
        
        latestDbSignature = await getLatestSignatureBeforeUnixEpoch(connection, account, latestSignatureUnixEpoch);
    } else {
        latestDbSignature = latestDbSignatureRow['signature'];
    }

    let newSignatures = await getNewSignatures(latestDbSignature, connection, account);

    // By default the signatures returned by getConfirmedSignaturesForAddress2 will be ordered newest -> oldest
    // We reverse the order to oldest -> newest here
    // This is useful for our purposes as by inserting oldest -> newest if inserts are interrupted for some reason the process can pick up where it left off seamlessly (with no gaps)
    // Also ensures that the auto increment id in our table is incremented oldest -> newest
    newSignatures = newSignatures.reverse();
    db.transaction(signaturesInfo => {
        const insert = db.prepare("INSERT INTO transactions (signature, account, block_time, block_datetime, slot, err, process_state) values (?, ?, ?,  datetime(?, 'unixepoch'), ?, ?, ?)");
        for (const signatureInfo of signaturesInfo) {
            insert.run(
                signatureInfo.signature,
                account.toBase58(),
                signatureInfo.blockTime,
                signatureInfo.blockTime,
                signatureInfo.slot,
                signatureInfo.err === null ? 0 : 1,
                'unprocessed'
                );
            }
    })(newSignatures);

    console.log('inserted ' + newSignatures.length + ' signatures')
}


function updateTransactionSummary(confirmedTransaction, signature) {
    let maxCompute = 0;
    for (let logMessage of confirmedTransaction.meta!.logMessages!) {
        
        if (logMessage.endsWith('compute units')) {
            let re = new RegExp(/(\d+)\sof/);
            let matches = re.exec(logMessage);
            if (matches) {
                let compute = parseInt(matches[1]);
                if (compute > maxCompute) {
                    maxCompute = compute;
                }
            }
        }
    }

    let logMessages = confirmedTransaction.meta!.logMessages!.join('\n');

    db.prepare("update transactions set log_messages = ?, compute = ? where signature = ?").run(
        logMessages, maxCompute, signature);
}


function toNumber(data, decimals) {
    return data.toNumber() / Math.pow(10, decimals)
}


function processOracleTransaction(confirmedTransaction, signature) {
    let oraclePk = confirmedTransaction.transaction.keys[1].toString()
    let slot = confirmedTransaction.slot;

    let instruction = schema_1.Instruction.deserialize(confirmedTransaction!.transaction.instructions[0].data);
    
    let roundId = instruction.Submit.round_id.toNumber();
    let value = toNumber(instruction.Submit.value, 2)
    let symbol = oracleSymbolMap[oraclePk];

    db.prepare("insert into oracle_transactions (signature, slot, oracle_pk, round_id, value, symbol) values (?, ?, ?, ?, ?, ?)").run(
        signature, slot, oraclePk, roundId, value, symbol);
}

function insertDepositWithdraws(depositWithdrawInserts, signature) {
    const insertDepositWithdrawSmt = db.prepare('INSERT INTO deposit_withdraw (signature, instruction_num, slot, owner, side, quantity, symbol) VALUES (?, ?, ?, ?, ?, ?, ?)');
    // This query get the latest price for each symbol in a signature
    const selectUsdEquivalentSmt = db.prepare(`
    with latest_slot as
    (
    select
    t1.signature,
    t1.symbol,
    max(t2.slot) as min_slot
    from deposit_withdraw t1
    left join oracle_transactions t2
    on t1.symbol = t2.symbol
    and t1.slot >= t2.slot
    group by t1.signature, t1.symbol
    )
    select t1.signature,
    t1.symbol,
    case 
    when t1.symbol = 'USDT' then 1 
    else t3.value
    end as symbol_price,
    case 
    when t1.symbol = 'USDT' then 1 
    else t3.value
    end * t1.quantity as usd_equivalent
    from deposit_withdraw t1
    inner join latest_slot t2
    on t2.signature = t1.signature
    left join oracle_transactions t3
    on t3.symbol = t2.symbol
    and t3.slot = t2.min_slot
    where t1.signature = ?
    `);
    // Can have multiple symbols per signature here but they will all have the same slot
    const updateDepositWithdrawSmt = db.prepare('update deposit_withdraw set symbol_price = ?, usd_equivalent = ? where signature = ? and symbol = ?')
    
    // There can be multiple deposits/withdraws in a Mango transaction
    // Insert them in a SQL transaction so that if one fails they all fail - otherwise could have process_state = error with some inserts completed (causing duplicates if signature re-processed)
    const insertDepositWithdrawTrans = db.transaction((depositWithdrawInserts) => {
        for (const depositWithdrawInsert of depositWithdrawInserts) insertDepositWithdrawSmt.run(depositWithdrawInsert);

        let updates = selectUsdEquivalentSmt.all(signature);

        for (const update of updates) updateDepositWithdrawSmt.run(update['symbol_price'], update['usd_equivalent'], update['signature'], update['symbol'])

    });
    insertDepositWithdrawTrans(depositWithdrawInserts);
}

function liquidationInserts(insertLiquidationsValues, insertLiquidationHoldingsValues) {
    const insertLiquidationSmt = db.prepare(`
    insert into liquidations 
    (signature, liqor, liquee, coll_ratio, in_token_symbol, in_token_amount, in_token_price, out_token_symbol, out_token_amount, out_token_price) 
    values 
    (@signature, @liqor, @liquee, @coll_ratio, @in_token_symbol, @in_token_amount, @in_token_price, @out_token_symbol, @out_token_amount, @out_token_price)`)

    const insertLiquidationHoldingsSmt = db.prepare(`
    insert into liquidation_holdings
    (signature, symbol, assets, liabs, price) 
    values 
    (@signature, @symbol, @assets, @liabs, @price)`)

    const insertLiquidationsTrans = db.transaction((insertLiquidationsValues, insertLiquidationHoldingsValues) => {
        insertLiquidationSmt.run(insertLiquidationsValues);

        for (const liquidationHoldings of insertLiquidationHoldingsValues) {
            insertLiquidationHoldingsSmt.run(liquidationHoldings)
        }
    });

    insertLiquidationsTrans(insertLiquidationsValues, insertLiquidationHoldingsValues)

}

function ParseLiquidation(instruction, confirmedTransaction, signature) {
    let accounts = instruction.keys.map(e => e.pubkey.toBase58())
    let mangoGroup, liqor, liqorInTokenWallet, liqorOutTokenWallet, liqeeMarginAccount, inTokenVault, outTokenVault, signerKey;
    [mangoGroup, liqor, liqorInTokenWallet, liqorOutTokenWallet, liqeeMarginAccount, inTokenVault, outTokenVault, signerKey] = accounts.slice(0, 8);

    let transactionMeta = confirmedTransaction.meta;

    let liqorInTokenWalletIndex = 2;
    let liqorOutTokenWalletIndex = 3;

    let inPreTokenBalances = transactionMeta.preTokenBalances.find(e => e.accountIndex === liqorInTokenWalletIndex);
    let inPostInTokenBalances = transactionMeta.postTokenBalances.find(e => e.accountIndex === liqorInTokenWalletIndex);
    let outPreOutTokenBalances = transactionMeta.preTokenBalances.find(e => e.accountIndex === liqorOutTokenWalletIndex);
    let outPostOutTokenBalances = transactionMeta.postTokenBalances.find(e => e.accountIndex === liqorOutTokenWalletIndex);
    
    // TODO: remove (testing)
    let inTokenSymbol = vaultSymbolMap[mangoGroup][inTokenVault];
    let outTokenSymbol = vaultSymbolMap[mangoGroup][outTokenVault];
    // let inTokenSymbol = 'BTC';
    // let outTokenSymbol = 'USDT';

    let inTokenAmount =  inPostInTokenBalances.uiTokenAmount.uiAmount - inPreTokenBalances.uiTokenAmount.uiAmount;
    let outTokenAmount = outPostOutTokenBalances.uiTokenAmount.uiAmount - outPreOutTokenBalances.uiTokenAmount.uiAmount;

    let assets;
    let liabs;
    let collRatio;
    let prices;
    let symbols;
    for (let logMessage of confirmedTransaction.meta.logMessages) {
        if (logMessage.startsWith('Program log: Liquidation details: ')) {
            let liquidationDetails = JSON.parse(logMessage.slice('Program log: Liquidation details: '.length));
            

            assets = liquidationDetails.assets;
            liabs = liquidationDetails.liabs;
            collRatio = liquidationDetails.coll_ratio;
            prices = liquidationDetails.prices;

            // TODO: remove
            // let mangoGroup = '7pVYhpKUHw88neQHxgExSH6cerMZ1Axx1ALQP9sxtvQV'

            symbols = mangoGroupSymbolMap[mangoGroup]

            let quoteDecimals = symbolMintDecimalsMap[mangoGroup][symbols[symbols.length - 1]]
            for (let i = 0; i < assets.length; i++) { 
                let symbol = symbols[i]
                let mintDecimals = symbolMintDecimalsMap[mangoGroup][symbol]

                prices[i] = prices[i] * Math.pow(10, mintDecimals - quoteDecimals)
                assets[i] = assets[i] / Math.pow(10, mintDecimals)
                liabs[i] = liabs[i] / Math.pow(10, mintDecimals)
            }
            
            break;
        }
    }

    let inTokenPrice = prices[symbols.indexOf(inTokenSymbol)];
    let outTokenPrice = prices[symbols.indexOf(outTokenSymbol)];

    let insertLiquidationsValues = {
        signature: signature,
        liqor: liqor,
        liquee: liqeeMarginAccount,
        coll_ratio: collRatio, 
        in_token_symbol: inTokenSymbol,
        in_token_amount: inTokenAmount,
        in_token_price: inTokenPrice,
        out_token_symbol: outTokenSymbol,
        out_token_amount: outTokenAmount,
        out_token_price: outTokenPrice
    }

    let insertLiquidationHoldingsValues: any[] = [];
    for (let i= 0; i < assets.length; i++) {
        insertLiquidationHoldingsValues.push({
            signature: signature,
            symbol: symbols[i],
            assets: assets[i],
            liabs: liabs[i],
            price: prices[i]
        })
    }

    liquidationInserts(insertLiquidationsValues, insertLiquidationHoldingsValues);
}

function processMangoTransaction(confirmedTransaction, signature) {
    let slot = confirmedTransaction.slot;
    let instructions = confirmedTransaction.transaction.instructions;

    // TODO: flow is not a great name - want name that covers withdrawals and deposits though
    let depositWithdrawInserts: any[] = [];
    // Can have multiple inserts per signature so add instructionNum column to allow a primary key
    let instructionNum = 1;
    for (let instruction of instructions) {
        let decodedInstruction = MangoInstructionLayout.decode(instruction.data);
        let instructionName = Object.keys(decodedInstruction)[0];

        if ((instructionName === 'Deposit') || (instructionName === 'Withdraw')) {
            // Luckily Deposit and Withdraw have the same layout

            let mangoGroup = instruction.keys[0].pubkey.toBase58()
            let owner = instruction.keys[2].pubkey.toBase58()
            let vault = instruction.keys[4].pubkey.toBase58()
            let symbol = vaultSymbolMap[mangoGroup][vault]
            let mintDecimals = symbolMintDecimalsMap[mangoGroup][symbol]
            let quantity = toNumber(decodedInstruction[instructionName].quantity, mintDecimals)
            
            depositWithdrawInserts.push([signature, instructionNum, slot, owner, instructionName, quantity, symbol])
        } else if (instructionName === 'PartialLiquidate') {
            ParseLiquidation(instruction, confirmedTransaction, signature)
        }
        instructionNum++;
    }

    // All deposits and withdraws in a mango transaction should be inserted in a single SQL transaction
    if (depositWithdrawInserts.length > 0) insertDepositWithdraws(depositWithdrawInserts, signature);

}

function processTransaction(confirmedTransaction) {
    let signature = this.signature;
    let account = this.account;

    try {
        updateTransactionSummary(confirmedTransaction, signature);

        // if transaction has an error - exit processing
        if (confirmedTransaction.meta.err !== null) {
            db.prepare("update transactions set process_state = 'transaction error' where signature = ?").run(signature);    
        } else {
            if (account === oracleProgramId) {
                processOracleTransaction(confirmedTransaction, signature);
            } else if (account == mangoProgramId){
                processMangoTransaction(confirmedTransaction, signature);
            }
    
            db.prepare("update transactions set process_state = 'processed' where signature = ?").run(signature);
        }

    } catch(e) {
        db.prepare("update transactions set process_state = 'processing error' where signature = ?").run(signature);
        console.log('error with signature: ' + signature);
    }
    
}

function createVaultSymbolMap(cluster) {
    // Create a mapping from vault pk to token symbol for each mango group
    let ids = IDS;
    let map = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]

        let mangoGroupPk = mangoGroupObj["mango_group_pk"]
        map[mangoGroupPk] = {};

        for (let symbol in mangoGroupObj.symbols) {
            let mintPk = mangoGroupObj.symbols[symbol];
            let mintIndex = mangoGroupObj.mint_pks.indexOf(mintPk);
            let vaultPk = mangoGroupObj.vault_pks[mintIndex];
            map[mangoGroupPk][vaultPk] = symbol;
        }
    }

    return map
}

function createOracleSymbolMap(cluster) {
    // Create a mapping from vault pk to token symbol for each mango group
    let ids = IDS;
    let map = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]

        for (let symbol in mangoGroupObj.symbols) {
            let mintPk = mangoGroupObj.symbols[symbol];
            let mintIndex = mangoGroupObj.mint_pks.indexOf(mintPk);
            // There are one less oracle than the number of tokens in the mango group
            if (mintIndex < mangoGroupObj.mint_pks.length - 1) {
                let oraclePk = mangoGroupObj.oracle_pks[mintIndex];
                map[oraclePk] = symbol;
            }
        }
    }

    return map
}

function CreateMangoGroupSymbolMap(cluster) {
    // Create a mapping from mango pk to array of token symbols
    let ids = IDS;

    let map = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]

        let mangoGroupPk = mangoGroupObj["mango_group_pk"]
        map[mangoGroupPk] = [];

        for (let mintPk of mangoGroupObj.mint_pks) {
            for (let symbol of Object.keys(mangoGroupObj.symbols)) {
                if (mangoGroupObj.symbols[symbol] === mintPk) {
                    map[mangoGroupPk].push(symbol);
                }
            }
        }

    }

    return map
}

function CreateSymbolMintDecimalsMap (cluster) {
    let ids = IDS;

    let map = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]
        
        let mangoGroupPk = mangoGroupObj["mango_group_pk"]
        map[mangoGroupPk] = {};
        for (let symbol of Object.keys(mangoGroupObj.symbols)) {
            // TODO: can I assume this is always true?
            map[mangoGroupPk][symbol] = symbol === "SOL" ? 9 : 6
            // TODO: remove (testing)
            // map[mangoGroupPk][symbol] = symbol === "USDT" ? 6 : 9
        }
    
    }

    return map
}


async function processTransactions(connection, account) {
    let signaturesToProcess = db.prepare("select signature from transactions where process_state = 'unprocessed' and account = ? order by id asc").all(account).map(e => e['signature']);

    let promises: Promise<void>[] = [];
    let counter = 1;
    for (let signature of signaturesToProcess) {
        let promise = connection.getConfirmedTransaction(signature).then(processTransaction.bind({signature: signature, account: account}));
        console.log('processed ', counter, ' of ', signaturesToProcess.length);
        counter++;
        
        promises.push(promise);

        // Limit request frequency to avoid request failures due to rate limiting    
        await sleep(requestWaitTime);
    }

    await (Promise as any).allSettled(promises);
}

async function main() {

    const dbInitPath = 'src/init.sql';

    // Init db if it doesn't exist
    let dbPath = path.join(__dirname, '..', 'logs.db');
    try {
        fs.accessSync(dbPath);
        db = new Database(dbPath);    
    } catch {
        const migration = fs.readFileSync(dbInitPath, 'utf8');
        db = new Database(dbPath);
        db.exec(migration);
        console.log('Database initialised');
    }
    db.pragma('journal_mode = WAL');

    const cluster = 'mainnet-beta';
    const clusterUrl = "https://api.mainnet-beta.solana.com";
    // const clusterUrl = "https://solana-api.projectserum.com";
    // const clusterUrl = "https://devnet.solana.com";
    const connection = new Connection(clusterUrl, 'finalized');


    // TODO: This is the program owner - for all oracles I think - does it make more sense to use this rather than the individual oracles?
    // Check with max that this won't change - it's not in ids.json so can't get it dynamically
    const oracleProgramPk = new PublicKey(oracleProgramId);
    const mangoProgramPk = new PublicKey(mangoProgramId);
    

    // TODO: Does this have to be a global variable?
    vaultSymbolMap = createVaultSymbolMap(cluster);
    oracleSymbolMap = createOracleSymbolMap(cluster);
    mangoGroupSymbolMap = CreateMangoGroupSymbolMap(cluster);
    symbolMintDecimalsMap = CreateSymbolMintDecimalsMap(cluster);

    // Order of inserting transactions important - inserting deposit_withdraw relies on having all oracle prices available
    // So get new signatures of oracle transactions after mango transactions and insert oracle transactions first
    await insertNewSignatures(mangoProgramPk, connection);
    await insertNewSignatures(oracleProgramPk, connection);
    await processTransactions(connection, oracleProgramId);
    await processTransactions(connection, mangoProgramId);

    // let signature = '2rcXkuHh2GD3Lisr8BDieyEu3xAK49x1EoMzxFh2jB6mR3zhZfNELaeeEgiUhhApyjxcJcpU63rEcMiHKsoMmDcE'
    // let signature = '3SU5y2dYT5LRHa9cSNG9voScDEAn8hZ16XQSNpQVjrk1zMuwQVhLbMS2WgQaEqCoRhoZ7Q4skZoDXWZyPz1nRZ43'
    // let confirmedTransaction = await connection.getConfirmedTransaction(signature);
    // processTransaction.bind({signature: signature, account: mangoProgramId})(confirmedTransaction);

    console.log('done')
}

main()
