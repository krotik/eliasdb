<!-- 
*
* EliasDB - Chat example
*
* Copyright 2019 Matthias Ladkau. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/.
*

Chat window which displays an ongoing chat channel.
-->
<template>
    <div>
        <div class="chat-msg-window" 
            v-for="msg in messages"
            v-bind:key="msg.key">
                <div>{{msg.message}} at {{new Date(msg.ts / 1000).toLocaleString()}}</div>
        </div>
        <chat-text-area :channel="channel"/>
    </div>
</template>

<script lang="ts">

import Vue from "vue";
import ChatTextArea from './ChatTextArea.vue';
import {EliasDBGraphQLClient} from "../lib/eliasdb-graphql";

interface Message {
    key: string
    message: string
}

export default Vue.extend({
    props: ['channel'],
    data() {
        return {
            client : new EliasDBGraphQLClient(),
            messages : [] as Message[],
        }
    },
    mounted: function () {

        // Ensure channel node exists

        this.client.req(`
mutation($node : NodeTemplate) {
  ${this.channel}(storeNode : $node) { }
}`,
            {
                node : {
                    key : this.channel,
                    kind : this.channel,
                }
            })
            .catch(e => {
                console.error("Could not join channel:", e);
            });

        // Start subscription

        this.client.subscribe(`
subscription {
  ${this.channel}(ascending:key, last:11) { # last:11 because channel node will be last
      key,
      message,
      ts,
  }
}`,
            data => {
                const messages = data.data[this.channel] as Message[];
                this.messages = messages.filter(m => !!m.message);
            });
    },
    components: {
        ChatTextArea,
    },
});
</script>

<style>
</style>
