/**
 * EliasDB - JavaScript GraphQL client library
 *
 * Copyright 2019 Matthias Ladkau. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 */
export enum RequestMetod {
    Post = 'post',
    Get = 'get'
}

export class EliasDBGraphQLClient {
    /**
     * Host this client is connected to.
     */
    protected host: string;

    /**
     * Partition this client is working on.
     */
    protected partition: string;

    /**
     * Websocket over which we can handle subscriptions.
     */
    private ws?: WebSocket;

    /**
     * EliasDB GraphQL endpoints.
     */
    private graphQLEndpoint: string;
    private graphQLReadOnlyEndpoint: string;

    /**
     * List of operations to execute once the websocket connection is established.
     */
    private delayedOperations: { (): void }[] = [];

    /**
     * Queue of subscriptions which await an id;
     */
    private subscriptionQueue: { (data: any): void }[] = [];

    /**
     * Map of active subscriptions.
     */
    private subscriptionCallbacks: { [id: string]: { (data: any): void } } = {};

    /**
     * Createa a new EliasDB GraphQL Client.
     *
     * @param host Host to connect to.
     * @param partition Partition to query.
     */
    public constructor(
        host: string = window.location.host,
        partition: string = 'main'
    ) {
        this.host = host;
        this.partition = partition;
        this.graphQLEndpoint = `https://${host}/db/v1/graphql/${partition}`;
        this.graphQLReadOnlyEndpoint = `https://${host}/db/v1/graphql-query/${partition}`;
    }

    /**
     * Initialize a websocket to support subscriptions.
     */
    private initWebsocket() {
        const url = `wss://${this.host}/db/v1/graphql-subscriptions/${this.partition}`;
        this.ws = new WebSocket(url);
        this.ws.onmessage = this.message.bind(this);

        this.ws.onopen = () => {
            if (this.ws) {
                this.ws.send(
                    JSON.stringify({
                        type: 'init',
                        payload: {}
                    })
                );
            }
        };
    }

    /**
     * Run a GraphQL query or mutation and return the response.
     *
     * @param query Query to run.
     * @param variables List of variable values. The query must define these
     *                  variables.
     * @param operationName Name of the named operation to run. The query must
     *                      specify this named operation.
     * @param method  Request method to use. Get requests cannot run mutations.
     */
    public req(
        query: string,
        variables: { [key: string]: any } = {},
        operationName: string = '',
        method: RequestMetod = RequestMetod.Post
    ): Promise<any> {
        const http = new XMLHttpRequest();

        const toSend: { [key: string]: any } = {
            operationName,
            variables,
            query
        };

        // Send an async ajax call

        if (method === RequestMetod.Post) {
            http.open(method, this.graphQLEndpoint, true);
        } else {
            const params = Object.keys(toSend)
                .map((key) => {
                    const val =
                        key !== 'variables'
                            ? toSend[key]
                            : JSON.stringify(toSend[key]);
                    return `${key}=${encodeURIComponent(val)}`;
                })
                .join('&');
            const url = `${this.graphQLReadOnlyEndpoint}?${params}`;

            http.open(method, url, true);
        }

        http.setRequestHeader('content-type', 'application/json');

        return new Promise(function (resolve, reject) {
            http.onload = function () {
                try {
                    if (http.status === 200) {
                        resolve(JSON.parse(http.response));
                    } else {
                        let err: string;
                        try {
                            err = JSON.parse(http.responseText)['errors'];
                        } catch {
                            err = http.responseText.trim();
                        }
                        reject(err);
                    }
                } catch (e) {
                    reject(e);
                }
            };

            if (method === RequestMetod.Post) {
                http.send(JSON.stringify(toSend));
            } else {
                http.send();
            }
        });
    }

    /**
     * Run a GraphQL subscription and receive updates if the data changes.
     *
     * @param query Query to run.
     * @param update Update callback.
     */
    public subscribe(
        query: string,
        update: (data: any) => void,
        variables: any = null
    ) {
        if (!this.ws) {
            this.initWebsocket();
        }

        if (this.ws) {
            const that = this;
            const subscribeCall = function () {
                if (that.ws) {
                    that.ws.send(
                        JSON.stringify({
                            id: that.subscriptionQueue.length,
                            query,
                            type: 'subscription_start',
                            variables
                        })
                    );
                    that.subscriptionQueue.push(update);
                }
            };

            if (this.ws.readyState !== WebSocket.OPEN) {
                this.delayedOperations.push(subscribeCall);
            } else {
                subscribeCall();
            }
        }
    }

    /**
     * Process a new websocket message.
     *
     * @param msg New message.
     */
    protected message(msg: MessageEvent) {
        const pmsg = JSON.parse(msg.data);

        if (pmsg.type == 'init_success') {
            // Execute the delayed operations

            this.delayedOperations.forEach((c) => c());
            this.delayedOperations = [];
        } else if (pmsg.type == 'subscription_success') {
            const callback = this.subscriptionQueue.shift();
            if (callback) {
                const id = pmsg.id;
                this.subscriptionCallbacks[id] = callback;
            }
        } else if (pmsg.type == 'subscription_data') {
            const callback = this.subscriptionCallbacks[pmsg.id];
            if (callback) {
                callback(pmsg.payload);
            }
        } else if (pmsg.type == 'subscription_fail') {
            console.error(
                'Subscription failed: ',
                pmsg.payload.errors.join('; ')
            );
        }
    }
}
