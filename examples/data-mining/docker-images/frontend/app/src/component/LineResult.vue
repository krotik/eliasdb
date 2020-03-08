<!--
*
* EliasDB - Data mining frontend example
*
* Copyright 2020 Matthias Ladkau. All rights reserved.
*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at http://mozilla.org/MPL/2.0/.
*

Graph which renders a result set as a line chart.
-->
<template>
  <div class="small">
    <line-chart :chart-data="linechartdata" :options="linechartoptions"></line-chart>
    <div class="chat-msg-window" v-for="msg in messages" v-bind:key="msg.key">
      <div>{{msg.result}}</div>
    </div>
  </div>
</template>

<script lang="ts">
import Vue from "vue";
import LineChart from "./LineChart.vue";
import { EliasDBGraphQLClient } from "../lib/eliasdb-graphql";

interface Message {
  key: string;
  result: string;
}

export default Vue.extend({
  props: ["name", "last"],
  data() {
    return {
      client: new EliasDBGraphQLClient(),
      messages: [] as Message[],
      linechartdata: null as any,
      linechartoptions: {
        responsive: true,
        maintainAspectRatio: false
      }
    };
  },
  mounted: async function() {
    // Ensure channel node exists

    let results: any[] = [];

    try {
      const response = await this.client.req(`
{
  PingResult(ascending: "key", last:${this.last}) {
    key
    result
    success
    url
  }
}`);
      results = JSON.parse(response).data.PingResult;
      console.log("Results:", results);
    } catch (e) {
      console.error("Could not query results:", e);
    }

    let labels: string[] = [];
    let data: number[] = [];

    results.forEach(r => {
      const timestamp = new Date();
      timestamp.setTime(parseInt(r["key"]) * 1000);
      const secs = parseFloat("0." + r["result"].split(".")[1]);
      if (!isNaN(secs)) {
        labels.push(timestamp.toISOString());
        data.push(secs);
      }
    });

    this.linechartdata = {
      labels: labels,
      datasets: [
        {
          label: this.name,
          backgroundColor: "#f87979",
          data: data,
          fill: false
        }
      ]
    };
  },
  components: {
    LineChart
  }
});
</script>

<style>
.small {
  margin: 50px auto;
}
</style>
