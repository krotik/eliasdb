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

Chat text area which lets the user enter new messages.
-->
<template>
    <div>
        <textarea class="chat-textarea"
            v-model="message" 
            v-on:keyup="eventKeyup"
            placeholder="Your message here ..."/>
        <input 
            value="Send" 
            title="Send message [CTRL+Return]"
            v-on:click="eventClick"
            type="button"/>
    </div>
</template>

<script lang="ts">

import Vue from "vue";
import {EliasDBGraphQLClient} from "../lib/eliasdb-graphql";

export default Vue.extend({
    props: ['channel'],
    data() {
        return {
            client : new EliasDBGraphQLClient(),
            message : "",
        }
    },
    mounted: function () {
        let input = document.querySelector('textarea.chat-textarea');
        if (input) {
            (input as HTMLTextAreaElement).focus();
        }
    },
    methods : {
        sendData() {
            if (this.message) {
                this.client.req(`
mutation($node : NodeTemplate) {
  ${this.channel}(storeNode : $node) { }
}`,
                    {
                        node : {
                            key : Date.now().toString(),
                            kind : this.channel,
                            message : this.message,
                        }
                    })
                    .catch(e => {
                        console.error("Could not join channel:", e);
                    });
                this.message = '';
            }
        },
        eventKeyup(event : KeyboardEvent) {
            if (event.keyCode === 13 && event.ctrlKey) {
                this.sendData();
            }
        },
        eventClick(event : KeyboardEvent) {
            this.sendData();
        }
    },
});
</script>

<style>
</style>
