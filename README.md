# Mango Instruction Stats

### TODO
- [x] Get all new transactions since last ran
- [x] Parse withdraws and deposits
- [x] Parse oracle prices
- [x] Assign oracle prices to withdraws and deposits
- [x] Parse liquidation and partial liquidation
- [x] Transition from sqlite to postgres

### Run
```
yarn install
yarn start
```

When the program is first run it will initialise the sqlite db and insert the last 6 hours of mango instructions and oracle price updates. Subsequent runs will insert all new transactions (since the previous run).
