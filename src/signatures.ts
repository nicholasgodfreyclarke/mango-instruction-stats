import { Connection, PublicKey, ConfirmedSignatureInfo } from '@solana/web3.js';
import { sleep} from '@blockworks-foundation/mango-client';
import { bulkBatchInsert } from './utils';


export async function getNewSignatures(afterSignature: string, connection: Connection, address: PublicKey, requestWaitTime) {
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
        
        let signaturesInfo = (await connection.getConfirmedSignaturesForAddress2(address, options));
        signatures = signaturesInfo.map(x => x['signature']);

        let afterSignatureIndex = signatures.indexOf(afterSignature);

        if (afterSignatureIndex !== -1) {
            allSignaturesInfo = allSignaturesInfo.concat(signaturesInfo.slice(0, afterSignatureIndex));
            break
        } else {
            // if afterSignatureIndex is not found then we should have gotten signaturesInfo of length limit
            // otherwise we have an issue where the rpc endpoint does not have enough history
            if (signaturesInfo.length !== limit) {
                throw 'rpc endpoint does not have sufficient signature history to reach afterSignature ' + afterSignature
            } 
            allSignaturesInfo = allSignaturesInfo.concat(signaturesInfo);
        }
        before = signatures[signatures.length-1];

        console.log(new Date(signaturesInfo[signaturesInfo.length-1].blockTime! * 1000).toISOString());

        await sleep(requestWaitTime);
    }

    return allSignaturesInfo
}


export async function getLatestSignatureBeforeUnixEpoch(connection, address, unixEpoch, requestWaitTime) {

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
        
        signaturesInfo = await connection.getConfirmedSignaturesForAddress2(address, options);

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


export async function insertNewSignatures(address, connection, pool, requestWaitTime) {
    let client = await pool.connect()
    let latestDbSignatureRows = await client.query('select signature from transactions where id = (select max(id) from transactions where account = $1)', [address.toBase58()])
    client.release();

    // let latestDbSignatureRow = db.prepare('select signature from transactions where id = (select max(id) from transactions where account = ?)').get(account.toBase58());

    let latestDbSignature;
    if (latestDbSignatureRows.rows.length === 0) {

        let currentUnixEpoch = Math.round(Date.now()/1000);
        // If tranasctions table is empty - initialise by getting all signatures in the 6 hours
        let latestSignatureUnixEpoch = currentUnixEpoch - 6 * 60 * 60;

        console.log('Current time ', new Date(currentUnixEpoch * 1000).toISOString());
        console.log('Getting all signatures after ', new Date(latestSignatureUnixEpoch * 1000).toISOString());
        
        latestDbSignature = await getLatestSignatureBeforeUnixEpoch(connection, address, latestSignatureUnixEpoch, requestWaitTime);
    } else {
        latestDbSignature = latestDbSignatureRows.rows[0]['signature'];
    }

    let newSignatures = await getNewSignatures(latestDbSignature, connection, address, requestWaitTime);

    // By default the signatures returned by getConfirmedSignaturesForAddress2 will be ordered newest -> oldest
    // We reverse the order to oldest -> newest here
    // This is useful for our purposes as by inserting oldest -> newest if inserts are interrupted for some reason the process can pick up where it left off seamlessly (with no gaps)
    // Also ensures that the auto increment id in our table is incremented oldest -> newest
    newSignatures = newSignatures.reverse();

    const inserts = newSignatures.map(signatureInfo => ({
                    signature: signatureInfo.signature,
                    account: address.toBase58(),
                    block_time: signatureInfo.blockTime,
                    block_datetime: new Date(signatureInfo.blockTime! * 1000),
                    slot: signatureInfo.slot,
                    err: signatureInfo.err === null ? 0 : 1,
                    process_state: 'unprocessed'
    }))
    let columns = ['signature', 'account', 'block_time', 'block_datetime', 'slot', 'err', 'process_state'];
    let table = 'transactions'
    let batchSize = 10000
    await bulkBatchInsert(pool, table, columns, inserts, batchSize);

    console.log('inserted ' + newSignatures.length + ' signatures')
}
