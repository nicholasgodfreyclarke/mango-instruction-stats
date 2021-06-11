import { Connection, PublicKey, ConfirmedTransaction, Transaction, ConfirmedSignatureInfo } from '@solana/web3.js';
import { MangoInstructionLayout, sleep, IDS, MangoClient} from '@blockworks-foundation/mango-client';
const schema_1 = require("@blockworks-foundation/mango-client/lib/schema.js");

import {createReverseIdsMap} from './maps';

import { insertNewSignatures } from './signatures';

import { Pool } from 'pg'

const pgp = require('pg-promise')({
    capSQL: true
 });

var reverseIds;

function ParseLiquidationData(instruction, confirmedTransaction) {
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
    
    let inTokenSymbol = reverseIds.vault_symbol[inTokenVault];
    let outTokenSymbol = reverseIds.vault_symbol[outTokenVault];

    let inTokenAmount =  (inPostInTokenBalances.uiTokenAmount.uiAmount - inPreTokenBalances.uiTokenAmount.uiAmount) * -1;
    let outTokenAmount = outPostOutTokenBalances.uiTokenAmount.uiAmount - outPreOutTokenBalances.uiTokenAmount.uiAmount;

    let symbols;
    let startAssets;
    let startLiabs;
    let endAssets;
    let endLiabs;
    let socializedLoss;
    let totalDeposits;
    let prices;
    let socializedLossPercentages: number[] = [];
    let startAssetsVal = 0;
    let startLiabsVal = 0;
    for (let logMessage of confirmedTransaction.meta.logMessages) {
        if (logMessage.startsWith('Program log: liquidation details: ')) {
            let liquidationDetails = JSON.parse(logMessage.slice('Program log: liquidation details: '.length));
            

            prices = liquidationDetails.prices;
            startAssets = liquidationDetails.start.assets;
            startLiabs = liquidationDetails.start.liabs;
            endAssets = liquidationDetails.end.assets;
            endLiabs = liquidationDetails.end.liabs;
            socializedLoss = liquidationDetails.socialized_losses;
            totalDeposits = liquidationDetails.total_deposits;

            symbols = reverseIds.mango_groups[mangoGroup].symbols
            // symbols = mangoGroupSymbolMap[mangoGroup]
            let quoteDecimals = reverseIds.mango_groups[mangoGroup].mint_decimals[symbols[symbols.length - 1]]
            for (let i = 0; i < startAssets.length; i++) { 
                let symbol = symbols[i]
                let mintDecimals = reverseIds.mango_groups[mangoGroup].mint_decimals[symbol]

                prices[i] = prices[i] * Math.pow(10, mintDecimals - quoteDecimals)
                startAssets[i] = startAssets[i] / Math.pow(10, mintDecimals)
                startLiabs[i] = startLiabs[i] / Math.pow(10, mintDecimals)
                endAssets[i] = endAssets[i] / Math.pow(10, mintDecimals)
                endLiabs[i] = endLiabs[i] / Math.pow(10, mintDecimals)
                totalDeposits[i] = totalDeposits[i] / Math.pow(10, mintDecimals)

                socializedLossPercentages.push(endLiabs[i] / totalDeposits[i])
                startAssetsVal += startAssets[i] * prices[i];
                startLiabsVal += startLiabs[i] * prices[i];
            }
            
            break;
        }
    }

    let collRatio = startAssetsVal / startLiabsVal;
    let inTokenPrice = prices[symbols.indexOf(inTokenSymbol)];
    let outTokenPrice = prices[symbols.indexOf(outTokenSymbol)];

    let inTokenUsd = inTokenAmount * inTokenPrice;
    let outTokenUsd = outTokenAmount * outTokenPrice;
    let liquidationFeeUsd = outTokenUsd - inTokenUsd;

    let liquidation = {
        mango_group: mangoGroup,
        liqor: liqor,
        liqee: liqeeMarginAccount,
        coll_ratio: collRatio,
        in_token_symbol: inTokenSymbol,
        in_token_amount: inTokenAmount,
        in_token_price: inTokenPrice,
        in_token_usd: inTokenUsd,
        out_token_symbol: outTokenSymbol,
        out_token_amount: outTokenAmount,
        out_token_price: outTokenPrice,
        out_token_usd: outTokenUsd,
        liquidation_fee_usd: liquidationFeeUsd,
        socialized_losses: socializedLoss,
    }

    let liquidationHoldings: any = [];
    for (let i= 0; i < startAssets.length; i++) {
        if ((startAssets[i] > 0) || (startLiabs[i] > 0)) {
            liquidationHoldings.push({
                symbol: symbols[i],
                start_assets: startAssets[i],
                start_liabs: startLiabs[i],
                end_assets: endAssets[i],
                end_liabs: endLiabs[i],
                price: prices[i]
            })
        }
    }

    
    let socializedLosses: any[] = [];
    if (socializedLoss) {
        for (let i= 0; i < totalDeposits.length; i++) {
            if (endLiabs[i] > 0) {
                socializedLoss.push({
                    symbol: symbols[i],
                    symbol_price: prices[i],
                    loss: endLiabs[i],
                    total_deposits: totalDeposits[i],
                    loss_percentage: socializedLossPercentages[i],
                    loss_usd: endLiabs[i] * prices[i],
                    total_deposits_usd: totalDeposits[i] * prices[i]
                })
            }
        }
    }

    return [liquidation, liquidationHoldings, socializedLosses]
}




function parseOracleData(confirmedTransaction) {
    let oraclePk = confirmedTransaction.transaction.keys[1].toString()
    let slot = confirmedTransaction.slot;

    let instruction = schema_1.Instruction.deserialize(confirmedTransaction!.transaction.instructions[0].data);
    
    let roundId = instruction.Submit.round_id.toNumber();
    let submitValue = instruction.Submit.value.toNumber()
    let decimals = reverseIds.oracle_decimals[oraclePk]
    let value = submitValue / Math.pow(10, decimals)
    let symbol = reverseIds.oracle_symbol[oraclePk];

    return {slot: slot, oracle_pk: oraclePk, round_id: roundId, value: value, symbol: symbol, submit_value: submitValue}
}


function parseDepositWithdrawData(instruction) {
    let decodedInstruction = MangoInstructionLayout.decode(instruction.data);
    let instructionName = Object.keys(decodedInstruction)[0];

    let mangoGroup = instruction.keys[0].pubkey.toBase58()
    let marginAccount = instruction.keys[1].pubkey.toBase58()
    let owner = instruction.keys[2].pubkey.toBase58()
    let vault = instruction.keys[4].pubkey.toBase58()
    let symbol = reverseIds.vault_symbol[vault]
    let mintDecimals = reverseIds.mango_groups[mangoGroup].mint_decimals[symbol]
    let quantity = decodedInstruction[instructionName].quantity.toNumber() / Math.pow(10, mintDecimals)

    return {mango_group: mangoGroup, owner: owner, quantity: quantity, symbol: symbol, side: instructionName, margin_account: marginAccount}

}

function parseMangoTransactions(transactions) {
    let processStates: any[] = [];
    let transactionSummaries: any[] = [];

    let depositWithdrawInserts: any[] = [];
    let liquidationInserts: any[] = [];
    let liquidationHoldingsInserts: any[] = [];
    let socializedLossInserts: any[] = [];

    let counter = 1;
    for (let transaction of transactions) {
        let [signature, confirmedTransaction] = transaction;
        try {
            let transactionSummary = parseTransactionSummary(confirmedTransaction)
            transactionSummary['signature'] = signature
            transactionSummaries.push(transactionSummary)

            if (confirmedTransaction.meta.err !== null) {
                processStates.push({signature: signature, process_state: 'transaction error'});
            } else {
                let slot = confirmedTransaction.slot;
                let instructions = confirmedTransaction.transaction.instructions;

                // Can have multiple inserts per signature so add instructionNum column to allow a primary key
                let instructionNum = 1;
                for (let instruction of instructions) {
                    
                    let decodedInstruction
                    try {
                        decodedInstruction = MangoInstructionLayout.decode(instruction.data);
                    } catch(e) {
                        // TODO: think about a better way of handling the situation where I try to decode a non mango instruction and get an error
                        // If I fail to parse a legitimate mango instruction then this will fail silently
                        console.log(e)
                    }
                        
                    if (decodedInstruction) {
                        let instructionName = Object.keys(decodedInstruction)[0];

                        if ((instructionName === 'Deposit') || (instructionName === 'Withdraw')) {
                            // Luckily Deposit and Withdraw have the same layout
    
                            let depositWithdrawData = parseDepositWithdrawData(instruction)
                            depositWithdrawData['signature'] = signature
                            depositWithdrawData['slot'] = slot
                            depositWithdrawData['instruction_num'] = instructionNum
                            
                            depositWithdrawInserts.push(depositWithdrawData);                        
    
                        } else if (instructionName === 'PartialLiquidate') {
                            let [liquidation, liquidationHoldings, socializedLosses] = ParseLiquidationData(instruction, confirmedTransaction)
                            liquidation['signature'] = signature
    
                            liquidationInserts.push(liquidation)
    
                            for (let liquidationHolding of liquidationHoldings) {
                                liquidationHolding['signature'] = signature
                                liquidationHoldingsInserts.push(liquidationHolding)
                            }
    
                            for (let socializedLoss of socializedLosses) {
                                socializedLoss['signature'] = signature
                                socializedLossInserts.push(socializedLoss)
                            }
    
                        }
                    }

                    instructionNum++;
                }

                processStates.push({signature: signature, process_state: 'processed'});
            }
        } catch(e) {
            processStates.push({signature: signature, process_state: 'processing error'});
        }
        counter++;
    }

    return [processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts]
}

function parseOracleTransactions(transactions) {
    let processStates: any[] = [];
    let transactionSummaries: any[] = [];
    let oracleTransactions: any[] = [];
    for (let transaction of transactions) {
        let [signature, confirmedTransaction] = transaction;
        try {
            let transactionSummary = parseTransactionSummary(confirmedTransaction)
            transactionSummary['signature'] = signature
            transactionSummaries.push(transactionSummary)

            if (confirmedTransaction.meta.err !== null) {
                processStates.push({signature: signature, process_state: 'transaction error'});
            } else {    
                let oracleTransaction = parseOracleData(confirmedTransaction)
                oracleTransaction['signature'] = signature
                oracleTransactions.push(oracleTransaction)
    
                processStates.push({signature: signature, process_state: 'processed'});
            }
        } catch(e) {
            processStates.push({signature: signature, process_state: 'processing error'});
        }
    }

    return [processStates, transactionSummaries, oracleTransactions]
}

async function getUnprocessedSignatures(pool, account) {
    const client = await pool.connect();
    let signatures;
    try {
        const res = await client.query("select signature from transactions where process_state = 'unprocessed' and account = $1 order by id asc", [account])
        signatures = res.rows.map(e => e['signature'])
      } finally {
        client.release()
    }   

    return signatures;
}

function parseTransactionSummary(confirmedTransaction) {
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

    return {log_messages: logMessages, compute: maxCompute} 
}

async function insertMangoTransactions(pool, processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts) {

    const processStateCs = new pgp.helpers.ColumnSet(['?signature', 'process_state'], {table: 'transactions'});
    const transactionSummaryCs  = new pgp.helpers.ColumnSet(['?signature', 'log_messages', 'compute'], {table: 'transactions'});

    const depositWithdrawCs  = new pgp.helpers.ColumnSet(['signature', 'mango_group', 'instruction_num', 'slot', 'owner', 'side', 'quantity', 'symbol', 'margin_account'], {table: 'deposit_withdraw'});

    const liquidationsCs = new pgp.helpers.ColumnSet(
        ['signature', 'mango_group', 'liqor', 'liqee', 'coll_ratio', 'in_token_symbol', 'in_token_amount', 'in_token_price', 'in_token_usd', 'out_token_symbol', 'out_token_amount', 
        'out_token_price', 'out_token_usd', 'liquidation_fee_usd', 'socialized_losses'],
        {table: 'liquidations'});
    const liquidationHoldingsCs = new pgp.helpers.ColumnSet(
        ['signature', 'symbol', 'start_assets', 'start_liabs', 'end_assets', 'end_liabs', 'price'],
        {table: 'liquidation_holdings'});
    const socializedLossesCs = new pgp.helpers.ColumnSet(
        ['signature', 'symbol', 'symbol_price', 'loss', 'total_deposits', 'loss_percentage', 'loss_usd', 'total_deposits_usd'],
        {table: 'socialized_losses'});

    
    let batchSize = 1000;
    let client = await pool.connect()
    try {
        await client.query('BEGIN')

        for (let i = 0, j = processStates.length; i < j; i += batchSize) {
            let updatesBatch = processStates.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, processStateCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = transactionSummaries.length; i < j; i += batchSize) {
            let updatesBatch = transactionSummaries.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, transactionSummaryCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = depositWithdrawInserts.length; i < j; i += batchSize) {
            let insertsBatch = depositWithdrawInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, depositWithdrawCs);
            await client.query(insertsSql)
        }

        for (let i = 0, j = liquidationInserts.length; i < j; i += batchSize) {
            let insertsBatch = liquidationInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, liquidationsCs);
            await client.query(insertsSql)
        }

        for (let i = 0, j = liquidationHoldingsInserts.length; i < j; i += batchSize) {
            let insertsBatch = liquidationHoldingsInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, liquidationHoldingsCs);
            await client.query(insertsSql)
        }

        for (let i = 0, j = socializedLossInserts.length; i < j; i += batchSize) {
            let insertsBatch = socializedLossInserts.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, socializedLossesCs);
            await client.query(insertsSql)
        }

        await client.query('COMMIT')
    } catch (e) {
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
}

async function insertOracleTransactions(pool, processStates, transactionSummaries, oracleTransactions) {
    // Oracle transactions are quite frequent - so update in batches here for performance
    
    const processStartCs = new pgp.helpers.ColumnSet(['?signature', 'process_state'], {table: 'transactions'});
    const transactionSummaryCs  = new pgp.helpers.ColumnSet(['?signature', 'log_messages', 'compute'], {table: 'transactions'});
    const oracleCs = new pgp.helpers.ColumnSet(['signature', 'slot', 'oracle_pk', 'round_id', 'value', 'symbol', 'submit_value'], {table: 'oracle_transactions'});
    
    let batchSize = 1000;
    let client = await pool.connect()
    try {
        await client.query('BEGIN')

        for (let i = 0, j = processStates.length; i < j; i += batchSize) {
            let updatesBatch = processStates.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, processStartCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = transactionSummaries.length; i < j; i += batchSize) {
            let updatesBatch = transactionSummaries.slice(i, i + batchSize);
            let updatedSql = pgp.helpers.update(updatesBatch, transactionSummaryCs) + ' WHERE v.signature = t.signature';
            await client.query(updatedSql)
        }

        for (let i = 0, j = oracleTransactions.length; i < j; i += batchSize) {
            let insertsBatch = oracleTransactions.slice(i, i + batchSize);
            let insertsSql = pgp.helpers.insert(insertsBatch, oracleCs);
            await client.query(insertsSql)
        }

        await client.query('COMMIT')
    } catch (e) {
        await client.query('ROLLBACK')
        throw e
    } finally {
        client.release()
    }
}

async function getNewAddressTransactions(connection, address, requestWaitTime, pool) {

    let signaturesToProcess = (await getUnprocessedSignatures(pool, address))
     
    let promises: Promise<void>[] = [];
    let transactions: any[] = [];
    let counter = 1;
    for (let signature of signaturesToProcess) {
        let promise = connection.getConfirmedTransaction(signature).then(confirmedTransaction => transactions.push([signature, confirmedTransaction]));
        console.log('requested ', counter, ' of ', signaturesToProcess.length);
        counter++;
        
        promises.push(promise);

        // Limit request frequency to avoid request failures due to rate limiting
        await sleep(requestWaitTime);
    }
    await (Promise as any).allSettled(promises);

    return transactions

}

async function processOracleTransactions(connection, address, pool, requestWaitTime) {

    let transactions = await getNewAddressTransactions(connection, address, requestWaitTime, pool)

    let [processStates, transactionSummaries, oracleTransactions] = parseOracleTransactions(transactions)
    
    await insertOracleTransactions(pool, processStates, transactionSummaries, oracleTransactions)

}

async function processMangoTransactions(connection, address, pool, requestWaitTime) {

    let transactions = await getNewAddressTransactions(connection, address, requestWaitTime, pool)

    let [processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts] = parseMangoTransactions(transactions)

    await insertMangoTransactions(pool, processStates, transactionSummaries, depositWithdrawInserts, liquidationInserts, liquidationHoldingsInserts, socializedLossInserts)

}


async function main() {
    const cluster = process.env.CLUSTER || 'mainnet-beta';
    const clusterUrl = process.env.CLUSTER_URL || "https://api.mainnet-beta.solana.com";
    const requestWaitTime = parseInt(process.env.REQUEST_WAIT_TIME!) || 500;
    const connectionString = process.env.CONNECTION_STRING
    const oracleProgramId = process.env.ORACLE_PROGRAM_ID || 'FjJ5mhdRWeuaaremiHjeQextaAw1sKWDqr3D7pXjgztv';
    const mangoProgramId = process.env.MANGO_PROGRAM_ID || '5fNfvyp5czQVX77yoACa3JJVEhdRaWjPuazuWgjhTqEH'
    
    const connection = new Connection(clusterUrl, 'finalized');
    const pool = new Pool(
        {
        connectionString: connectionString,
        ssl: {
            rejectUnauthorized: false,
        }
        }
    )

    // TODO: This is the program owner - for all oracles I think - does it make more sense to use this rather than the individual oracles?
    // Check with max that this won't change - it's not in ids.json so can't get it dynamically
    const oracleProgramPk = new PublicKey(oracleProgramId);
    const mangoProgramPk = new PublicKey(mangoProgramId);
    
    reverseIds = await createReverseIdsMap(cluster, new MangoClient(), connection);

    // Order of inserting transactions important - inserting deposit_withdraw relies on having all oracle prices available
    // So get new signatures of oracle transactions after mango transactions and insert oracle transactions first
    await insertNewSignatures(mangoProgramPk, connection, pool, requestWaitTime);
    await insertNewSignatures(oracleProgramPk, connection, pool, requestWaitTime);
    await processOracleTransactions(connection, oracleProgramId, pool, requestWaitTime);
    await processMangoTransactions(connection, mangoProgramId, pool, requestWaitTime);

    console.log('done')
}

main()
