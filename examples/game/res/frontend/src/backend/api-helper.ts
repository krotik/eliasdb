/**
 * EliasDB - JavaScript ECAL client library
 *
 * Copyright 2021 Matthias Ladkau. All rights reserved.
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

export class BackendClient {
    /**
     * Host this client is connected to.
     */
    protected host: string;

    /**
     * API endpoint for this client.
     */
    protected apiEndpoint: string;

    /**
     * Websocket endpoint for this client.
     */
    protected sockEndpoint: string;

    public constructor(host: string = window.location.host) {
        this.host = host;
        this.apiEndpoint = `https://${host}/db/ecal`;
        this.sockEndpoint = `wss://${host}/db/sock`;
    }

    /**
     * Process a new websocket message. This function expects
     * to have a bound update function on its context object.
     *
     * @param msg New message.
     */
    protected message(msg: MessageEvent) {
        const pmsg = JSON.parse(msg.data);

        if (pmsg.type == 'init_success') {
            console.log('New ECAL websocket established');
        } else if (pmsg.type == 'data') {
            (this as any).update(pmsg.payload);
        }
    }

    /**
     * Create and open a new websocket to the ECAL backend.
     *
     * @param path Websocket path.
     * @param data URL query data for the initial request.
     * @param update Update callback.
     *
     * The data parameter of the update callback has the following form:
     * {
     *     close: Boolean if the server closes the connection
     *     commID: Unique communication ID which the server uses for tracking
     *     payload: Payload data (defined by ECAL backend script)
     * }
     */
    public async createSock(
        path: string,
        data: any,
        update: (data: any) => void
    ): Promise<WebSocket> {
        const params = Object.keys(data)
            .map((key) => {
                const val = data[key];
                return `${key}=${encodeURIComponent(val)}`;
            })
            .join('&');

        const url = `${this.sockEndpoint}${path}?${params}`;
        const boundMessageFunc = this.message.bind({
            update
        });

        return new Promise(function (resolve, reject) {
            try {
                const ws = new WebSocket(url);

                ws.onmessage = boundMessageFunc;
                ws.onopen = () => {
                    resolve(ws);
                };
            } catch (err) {
                reject(err);
            }
        });
    }

    /**
     * Send data over an existing websocket
     */
    public sendSockData(ws: WebSocket, data: any) {
        ws.send(JSON.stringify(data));
    }

    /**
     * Send a request to the ECAL backend.
     *
     * @param query Query to run.
     * @param variables List of variable values. The query must define these
     *                  variables.
     * @param operationName Name of the named operation to run. The query must
     *                      specify this named operation.
     * @param method  Request method to use. Get requests cannot run mutations.
     */
    public req(
        path: string,
        data: any,
        method: RequestMetod = RequestMetod.Post
    ): Promise<any> {
        const http = new XMLHttpRequest();

        if (method === RequestMetod.Post) {
            http.open(method, `${this.apiEndpoint}${path}`, true);
        } else {
            const params = Object.keys(data)
                .map((key) => {
                    const val = data[key];
                    return `${key}=${encodeURIComponent(val)}`;
                })
                .join('&');
            const url = `${this.apiEndpoint}${path}?${params}`;

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
                            err =
                                JSON.parse(http.responseText)['error'] ||
                                http.responseText.trim();
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
                http.send(JSON.stringify(data));
            } else {
                http.send();
            }
        });
    }
}
