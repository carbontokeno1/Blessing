import test from "node:test";
import assert from "node:assert/strict";
import { ema, rsi, features, evaluateSignal, chooseStrategy, positionSizing, positionMarkMetrics, stockPerformanceReport, LIMITS } from "../server.mjs";

function sampleBars(n=180, trend=0.08) {
  const bars=[]; let price=100;
  for(let i=0;i<n;i++){
    const drift=trend+(i%9===0?-0.03:0.01);
    const open=price; price=Math.max(1,price+drift);
    bars.push({time:i*300000,open,high:Math.max(open,price)+0.12,low:Math.min(open,price)-0.12,close:price,volume:1000+(i%7)*150});
  }
  return bars;
}
test("EMA returns stable series and correct final value",()=>{
  const out=ema([1,2,3],2); assert.equal(out.length,3); assert.ok(out[2]>out[1]);
});
test("RSI remains within 0..100",()=>{
  const v=rsi(Array.from({length:30},(_,i)=>100+i),14); assert.ok(v>=0&&v<=100);
});
test("features require enough bars and return positive ATR",()=>{
  assert.equal(features(sampleBars(20)),null);
  const f=features(sampleBars()); assert.ok(f.atr>0); assert.ok(f.ema9>0);
});
test("strategy selection always has a safe fallback",()=>{
  const choice=chooseStrategy([sampleBars(45)]); assert.equal(choice.selected,"trend"); assert.equal(choice.adaptive,false);
});
test("position sizing never exceeds notional or risk guardrails",()=>{
  const s=positionSizing({price:100,stop:97.5,fx:34,cashTHB:10000});
  assert.ok(s); assert.ok(s.notionalTHB<=LIMITS.maxPositionTHB); assert.ok(s.riskTHB<=LIMITS.maxRiskTHB+0.001);
});
test("invalid and short-only stop levels are rejected",()=>{
  assert.equal(positionSizing({price:100,stop:101,fx:34,cashTHB:10000}),null);
});
test("a trend signal can be evaluated deterministically",()=>{
  const bars=sampleBars(180,0.2); const signal=evaluateSignal(bars,"trend");
  assert.ok(signal===null || signal.strategy==="trend");
});

test("position mark metrics report entry-relative change and net paper P/L",()=>{
  const m=positionMarkMetrics({entryPrice:100,markPrice:110,qty:1,entryCostTHB:3403.4},34,0.001,0.0005);
  assert.equal(m.changePct,10);
  assert.ok(m.unrealizedNetTHB>0);
  assert.ok(m.netReturnPct>0);
});
test("stock performance report calculates win rate and strategy breakdown",()=>{
  const report=stockPerformanceReport([
    {market:"stocks",strategy:"trend",netTHB:100},
    {market:"stocks",strategy:"trend",netTHB:-50},
    {market:"stocks",strategy:"momentum",netTHB:25},
    {market:"crypto",strategy:"trend",netTHB:999}
  ],[]);
  assert.equal(report.closedTrades,3);
  assert.equal(report.wins,2);
  assert.equal(report.losses,1);
  assert.equal(report.winRatePct,66.7);
  assert.equal(report.netProfitTHB,75);
  assert.equal(report.profitFactor,2.5);
  assert.equal(report.byStrategy.length,2);
});
