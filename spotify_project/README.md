Run like this:

```bash
python3 main.py -m std (or kmeans)
```

To read from Spotify API directly you need to add argument: -k keys.json, where keys.json is 

```json
{
    "client_id" : "your-client-id",
    "client_secret" : "your-secret"
}
```