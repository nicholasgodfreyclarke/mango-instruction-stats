import { IDS} from '@blockworks-foundation/mango-client';
import { PublicKey} from '@solana/web3.js';


export async function createReverseIdsMap(cluster, client, connection) {

    let reverseIds = {}
    let ids = IDS;

    // vault - symbol map
    let vaultSymbolMap = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]

        for (let symbol in mangoGroupObj.symbols) {
            let mintPk = mangoGroupObj.symbols[symbol];
            let mintIndex = mangoGroupObj.mint_pks.indexOf(mintPk);
            let vaultPk = mangoGroupObj.vault_pks[mintIndex];
            vaultSymbolMap[vaultPk] = symbol;
        }
    }
    reverseIds['vault_symbol'] = vaultSymbolMap;

    // oracles - symbol map
    let oracleSymbolMap = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]

        for (let symbol in mangoGroupObj.symbols) {
            let mintPk = mangoGroupObj.symbols[symbol];
            let mintIndex = mangoGroupObj.mint_pks.indexOf(mintPk);
            // There are one less oracle than the number of tokens in the mango group
            if (mintIndex < mangoGroupObj.mint_pks.length - 1) {
                let oraclePk = mangoGroupObj.oracle_pks[mintIndex];
                oracleSymbolMap[oraclePk] = symbol;
            }
        }
    }
    reverseIds['oracle_symbol'] = oracleSymbolMap;


    // mangoGroup - symbols-array map
    

    let mangoGroupMap = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]

        let mangoGroupPk = mangoGroupObj["mango_group_pk"]
        mangoGroupMap[mangoGroupPk] = {};

        
        let symbols: string[] = []
        for (let mintPk of mangoGroupObj.mint_pks) {
            for (let symbol of Object.keys(mangoGroupObj.symbols)) {
                if (mangoGroupObj.symbols[symbol] === mintPk) {
                    symbols.push(symbol)
                    // mangoGroupSymbols[mangoGroupPk].push(symbol);
                }
            }
        }

        mangoGroupMap[mangoGroupPk]['symbols'] = symbols 


    }
    reverseIds['mango_groups'] = mangoGroupMap;

    let oracleDecimalsMap = {};
    for (let mangoGroupName in ids[cluster].mango_groups) {
        let mangoGroupObj = ids[cluster].mango_groups[mangoGroupName]
        let mangoGroupPk = mangoGroupObj.mango_group_pk;

        let mangoGroup = await client.getMangoGroup(connection, new PublicKey(mangoGroupPk));

        for (let i = 0; i < mangoGroup.oracles.length; i++) {
            oracleDecimalsMap[mangoGroup.oracles[i].toBase58()] = mangoGroup.oracleDecimals[i]
        }

        let symbols = reverseIds['mango_groups'][mangoGroupPk].symbols;
        let map = {}
        for (let i = 0; i < symbols.length; i++) {
            map[symbols[i]] = mangoGroup.mintDecimals[i]
        }
        reverseIds['mango_groups'][mangoGroupPk]['mint_decimals'] = map;

    }
    reverseIds['oracle_decimals'] = oracleDecimalsMap;

    return reverseIds
    
}
