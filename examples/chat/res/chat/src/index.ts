import Vue from 'vue';
import ChatWindow from './component/ChatWindow.vue';

let v = new Vue({
    el: '#app',
    template: `
    <div>
        <chat-window :channel="channel" />
    </div>
    `,
    data() {
        return {
            channel: 'general',
        };
    },
    components: {
        ChatWindow,
    },
});
