EliasDB Chat Example
==
This example demonstrates a simple application which uses advanced features of EliasDB:
- Node modification via ECAL script
- User Management
- GraphQL subscriptions

The tutorial assumes you have downloaded EliasDB, extracted and build it. For this tutorial please execute "start.sh" or "start.bat" in the subdirectory: examples/chat

After starting EliasDB point your browser to:
```
https://localhost:9090
```

The generated default key and certificate for https are self-signed which should give a security warning in the browser. After accepting you should see a login prompt. Enter the credentials for the default user elias:
```
Username: elias
Password: elias
```

The browser should display the chat application after clicking `Login`. Open a second window and write some chat messages. You can see that both windows update immediately. This is done with GraphQL subscriptions.
