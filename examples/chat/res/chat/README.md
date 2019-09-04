Chat Example
--
The chat is an example application demonstrating EliasDB's GraphQL interface. The application uses mutation operations to create chat messages and a subscription to receive new messages.

The subscription uses a WebSocket which is used to "push" new messages from the server to the client. As soon as a client sends a new message to the server the subscription ensures that all clients are updated.

The chat application comes as a compiled .js file in the dist/ directory and should work out of the box.

Point a browser to: https://localhost:9090

To rebuild the application use yarn:

First install all necessary dependencies:
```
yarn
```
Then build the application:
```
yarn build
```
