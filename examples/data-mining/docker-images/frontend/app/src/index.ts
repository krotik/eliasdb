import Vue from 'vue';
import LineResult from './component/LineResult.vue';

let v = new Vue({
    el: '#app',
    template: `
    <div>
        <h1>Ping results for devt.de</h1>
        <line-result :name="name" :last="last" />
    </div>
    `,
    data() {
        return {
            name: 'Ping Result',
            last: '50',
        };
    },
    components: {
        LineResult,
    },
});
