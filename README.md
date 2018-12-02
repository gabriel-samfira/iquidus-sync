# iquidus-sync
Alternate sync method for iquidus blockchain explorer. Tested only on TokenPay. At the current block height, this script should finish sync in less than 1 hour.

## Install

```bash
pip install -r requirements.txt
```

## Run

```bash
./explorer_sync.py --explorer-config $HOME/explorer/settings.json
```

If you prefer to log to file:

```bash
./explorer_sync.py --explorer-config $HOME/explorer/settings.json --log-file=/tmp/sync.log
```
