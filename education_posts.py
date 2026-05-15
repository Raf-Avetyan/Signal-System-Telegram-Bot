def _education_post(topic, body):
    return f"🎓 <b>Member Education</b>\n<blockquote>Topic: {topic}</blockquote>\n{body}"


def _interleave_post_groups(groups):
    remaining = [list(group) for group in groups if group]
    mixed = []
    while remaining:
        next_round = []
        for group in remaining:
            if group:
                mixed.append(group.pop(0))
            if group:
                next_round.append(group)
        remaining = next_round
    return mixed


def _risk_sizing_posts():
    posts = []
    accounts = [5_000, 10_000, 20_000, 50_000, 100_000]
    risk_pcts = [0.5, 1.0, 1.5]
    stop_cycle = [0.7, 0.9, 1.2, 1.5, 1.8]
    entry_price = 80_000.0
    for account in accounts:
        for risk_pct in risk_pcts:
            stop_pct = stop_cycle[len(posts) % len(stop_cycle)]
            risk_usd = account * risk_pct / 100.0
            notional = risk_usd / (stop_pct / 100.0)
            stop_price = entry_price * (1.0 - stop_pct / 100.0)
            body = (
                f"On a ${account:,.0f} account, risking {risk_pct:.1f}% means the maximum planned loss is ${risk_usd:,.0f}. "
                f"If BTC is bought at ${entry_price:,.0f} and the stop is {stop_pct:.1f}% lower at ${stop_price:,.0f}, the position notional should be about ${notional:,.0f} because {stop_pct:.1f}% of that size equals your allowed loss. "
                f"This is professional sizing: start from invalidation and dollar risk first, and only then decide whether the trade is worth taking."
            )
            posts.append(_education_post("Risk Management: Position Sizing", body))
    return posts


def _daily_limit_posts():
    posts = []
    accounts = [5_000, 10_000, 20_000, 50_000, 100_000]
    daily_caps = [1.5, 2.0, 2.5]
    trade_risks = [0.5, 0.75, 1.0]
    for account in accounts:
        for idx, day_cap_pct in enumerate(daily_caps):
            trade_risk_pct = trade_risks[idx]
            day_cap_usd = account * day_cap_pct / 100.0
            trade_risk_usd = account * trade_risk_pct / 100.0
            full_losses = day_cap_usd / max(trade_risk_usd, 1e-9)
            body = (
                f"If the daily loss limit is {day_cap_pct:.1f}% on a ${account:,.0f} account, the hard stop for the day is ${day_cap_usd:,.0f}. "
                f"When each trade risks {trade_risk_pct:.2f}% or about ${trade_risk_usd:,.0f}, you can only absorb roughly {full_losses:.1f} full losing trades before discipline must take over. "
                f"Professionals stop before revenge trading starts, because one emotional trade after the daily limit often does more damage than the first two losses combined."
            )
            posts.append(_education_post("Risk Management: Daily Loss Limit", body))
    return posts


def _expectancy_posts():
    posts = []
    win_rates = [35, 40, 45, 50, 55]
    avg_wins = [1.6, 1.9, 2.2]
    avg_loss = 1.0
    monthly_trades = 25
    for win_rate in win_rates:
        for avg_win in avg_wins:
            expectancy = (win_rate / 100.0) * avg_win - ((100.0 - win_rate) / 100.0) * avg_loss
            monthly_r = expectancy * monthly_trades
            body = (
                f"A system with a {win_rate:.0f}% win rate, average win of {avg_win:.1f}R, and average loss of 1.0R has an expectancy of about {expectancy:+.2f}R per trade. "
                f"Over {monthly_trades} trades, that is roughly {monthly_r:+.1f}R before fees and slippage, which is why a trader can still be profitable even when more than half of the trades lose. "
                f"The lesson is simple: the real edge is not just win rate, but the relationship between average loss, average win, and execution quality."
            )
            posts.append(_education_post("Performance Math: Expectancy", body))
    return posts


def _leverage_posts():
    posts = []
    accounts = [5_000, 10_000, 20_000, 50_000, 100_000]
    leverages = [5, 10, 20]
    stop_cycle = [0.8, 1.0, 1.4, 1.6, 2.0]
    for account in accounts:
        for leverage in leverages:
            risk_usd = account * 0.01
            stop_pct = stop_cycle[len(posts) % len(stop_cycle)]
            notional = risk_usd / (stop_pct / 100.0)
            margin_used = notional / leverage
            body = (
                f"On a ${account:,.0f} account risking 1.0%, the maximum trade loss is ${risk_usd:,.0f}. "
                f"If the stop is {stop_pct:.1f}%, the position size is about ${notional:,.0f}, and with {leverage}x leverage the margin tied up is about ${margin_used:,.0f}. "
                f"The professional point is that leverage changes margin efficiency, not the allowed loss; if leverage makes you trade bigger than the stop-based size, risk control is already broken."
            )
            posts.append(_education_post("Leverage: Margin Efficiency", body))
    return posts


def _scale_out_posts():
    posts = []
    split_sets = [(50, 25, 25), (40, 30, 30), (33, 33, 34), (25, 25, 50), (60, 20, 20)]
    tp_sets = [(1.0, 2.0, 3.0), (1.2, 2.0, 3.5), (0.8, 1.6, 2.8)]
    for split in split_sets:
        for tp1_r, tp2_r, tp3_r in tp_sets:
            s1, s2, s3 = [x / 100.0 for x in split]
            be_result = s1 * tp1_r + s2 * tp2_r
            full_result = s1 * tp1_r + s2 * tp2_r + s3 * tp3_r
            body = (
                f"With a {split[0]}/{split[1]}/{split[2]} scale-out split and targets at {tp1_r:.1f}R, {tp2_r:.1f}R, and {tp3_r:.1f}R, the trade still books about {be_result:.2f}R if TP1 and TP2 hit and the runner returns to breakeven. "
                f"If all three targets are reached, the full blended result is about {full_result:.2f}R. "
                f"This is why partial profit-taking can be useful: it protects realized gains while still keeping part of the position available for the larger move."
            )
            posts.append(_education_post("Trade Management: Scaling Out", body))
    return posts


def _session_and_news_posts():
    posts = []
    session_cases = [
        ("London open", 180, 520, 30),
        ("New York open", 220, 640, 35),
        ("Asia range", 140, 310, 25),
        ("CPI release", 260, 780, 10),
        ("FOMC hour", 320, 950, 15),
    ]
    tactics = [
        "If the first impulse is already multiple ATR, the cleaner trade is often the retrace, not the chase.",
        "Waiting a few candles can cost some price, but it often saves the whole trade idea from being entered at the worst location.",
        "Professionals usually care more about post-event structure than about being first into the move.",
    ]
    for session_name, atr_15m, impulse, wait_minutes in session_cases:
        for note in tactics:
            atr_multiple = impulse / max(atr_15m, 1)
            body = (
                f"If 15m ATR is around ${atr_15m:,.0f} and {session_name.lower()} prints a ${impulse:,.0f} impulse, that move is already about {atr_multiple:.1f} ATR. "
                f"In practice, many traders are better off waiting at least {wait_minutes} minutes for a pullback, reclaim, or failed continuation instead of entering inside the first emotional expansion. "
                f"{note}"
            )
            posts.append(_education_post("Sessions and News: Volatility Control", body))
    return posts


def _structure_and_liquidity_posts():
    posts = []
    scenarios = [
        (79_200, 78_850, 80_100, 81_300),
        (80_000, 79_450, 80_350, 81_600),
        (81_100, 80_400, 81_550, 82_700),
        (78_600, 77_900, 79_050, 80_200),
        (82_000, 81_250, 82_480, 83_400),
    ]
    frames = ["15m reclaim", "1H reclaim", "15m sweep", "1H support hold", "4H level retest"]
    closes = [0.15, 0.25, 0.35]
    for base, sweep, reclaim, target in scenarios:
        for close_buffer in closes:
            frame = frames[len(posts) % len(frames)]
            confirm = reclaim - base
            body = (
                f"Imagine BTC loses ${base:,.0f}, sweeps into ${sweep:,.0f}, then reclaims around ${reclaim:,.0f}. "
                f"If the close holds at least {close_buffer:.2f}% above the broken level, the market has shown roughly ${confirm:,.0f} of reclaim distance before targeting liquidity near ${target:,.0f}. "
                f"This is the kind of structure example traders should map before entry: where liquidity was taken, where acceptance returned, and where the next obvious magnet sits."
            )
            posts.append(_education_post(f"Structure and Liquidity: {frame}", body))
    return posts


def _drawdown_posts():
    posts = []
    drawdowns = [5, 8, 10, 12, 15]
    recovery_risks = [0.5, 0.75, 1.0]
    for drawdown in drawdowns:
        for risk_pct in recovery_risks:
            recovery_pct = (1.0 / (1.0 - drawdown / 100.0) - 1.0) * 100.0
            approx_two_r_wins = drawdown / max(risk_pct * 2.0, 1e-9)
            body = (
                f"A {drawdown:.0f}% drawdown needs about {recovery_pct:.1f}% just to get back to breakeven. "
                f"If recovery mode uses only {risk_pct:.2f}% risk per trade and the average winner is 2R, it still takes roughly {approx_two_r_wins:.1f} clean winning trades to repair that damage. "
                f"This is why professionals cut size during drawdown: recovery comes from consistency and stability, not from trying to win everything back in one oversized trade."
            )
            posts.append(_education_post("Drawdown Control and Recovery", body))
    return posts


def _journal_posts():
    posts = []
    trade_counts = [20, 30, 40, 50, 60]
    win_rates = [38, 45, 52]
    avg_wins = [1.8, 2.0, 2.2]
    lunch_losses = [4, 6, 8, 10, 12]
    for idx, total_trades in enumerate(trade_counts):
        for win_rate, avg_win in zip(win_rates, avg_wins):
            expectancy = (win_rate / 100.0) * avg_win - ((100.0 - win_rate) / 100.0) * 1.0
            lunch_loss_count = lunch_losses[(idx + int(avg_win * 10)) % len(lunch_losses)]
            body = (
                f"If a journal shows {total_trades} trades, a {win_rate:.0f}% win rate, and an average win of {avg_win:.1f}R against a 1R loss, the expectancy is about {expectancy:+.2f}R per trade. "
                f"If {lunch_loss_count} of the weakest trades came during dead midday conditions, the first improvement is obvious: remove that time window before changing the whole strategy. "
                f"Journaling is useful because it turns vague frustration into measurable leaks that can actually be fixed."
            )
            posts.append(_education_post("Trade Review and Journaling", body))
    return posts


def _execution_posts():
    posts = []
    planned_entries = [79_800, 80_000, 80_250, 80_600, 81_000]
    slippages = [80, 150, 250]
    stop_offsets = [450, 600, 750]
    for entry in planned_entries:
        for idx, slip in enumerate(slippages):
            stop_offset = stop_offsets[idx]
            planned_stop = entry - stop_offset
            filled_entry = entry + slip
            planned_risk = entry - planned_stop
            actual_risk = filled_entry - planned_stop
            risk_increase = actual_risk - planned_risk
            body = (
                f"If the planned long entry is ${entry:,.0f} with a stop at ${planned_stop:,.0f}, the planned risk per coin is ${planned_risk:,.0f}. "
                f"If a late market order fills ${slip:,.0f} higher at ${filled_entry:,.0f}, the real risk becomes ${actual_risk:,.0f}, which means risk widened by about ${risk_increase:,.0f} without improving the idea at all. "
                f"This is why good execution matters: chasing changes the risk-reward math even when the chart idea stays the same."
            )
            posts.append(_education_post("Execution Quality and Slippage", body))
    return posts


def _rr_and_filter_posts():
    posts = []
    risk_examples = [
        (420, 1.5),
        (550, 1.8),
        (700, 2.0),
        (850, 2.4),
        (1_000, 3.0),
    ]
    win_rates = [35, 40, 45]
    for risk_dollars, rr_target in risk_examples:
        for win_rate in win_rates:
            reward = risk_dollars * rr_target
            expectancy = (win_rate / 100.0) * rr_target - ((100.0 - win_rate) / 100.0) * 1.0
            body = (
                f"When a trade risks ${risk_dollars:,.0f} to make {rr_target:.1f}R, the gross reward is about ${reward:,.0f} if the plan completes. "
                f"At a {win_rate:.0f}% win rate that setup carries an expectancy near {expectancy:+.2f}R, so the real question is not whether the trade can win, but whether the reward is large enough for the risk being taken. "
                f"Professionals filter hard here, because weak R multiples slowly poison the account even when the chart reading is decent."
            )
            posts.append(_education_post("Risk-Reward Filtering", body))
    return posts


def _actionable_playbook_posts():
    playbooks = [
        (
            "Execution Playbook: Reclaim Long",
            "BTC trades below $80,000, sweeps to $79,350, then closes back above $80,050 on 15m. "
            "What to do: 1. Mark the sweep low at $79,350. 2. Wait for a 15m close back above the broken level. 3. Enter on the first small pullback into $80,000-$80,080, not in the middle of the recovery candle. 4. Place the stop below the sweep low, for example near $79,240. 5. If the first target is prior intraday liquidity around $80,900, the reward is about $820 against roughly $810 risk, so only keep the trade if higher targets like $81,350 are realistic too."
        ),
        (
            "Execution Playbook: Breakdown Short",
            "BTC loses $81,200 support and closes 15m below it at $81,020. "
            "How to do it: 1. Do not short the first breakdown candle if it already traveled $350-$500. 2. Wait for a retest into $81,120-$81,220. 3. If price rejects there and cannot close back above support, short the failed retest. 4. A clean stop can sit near $81,360. 5. If the first downside target is $80,250, the trade risks about $180-$240 to make roughly $800-$900, which is the kind of imbalance worth waiting for."
        ),
        (
            "Execution Playbook: Sweep Reversal",
            "Suppose BTC is ranging between $79,800 and $80,900. "
            "What to do: 1. Let price take one side of the range first. 2. If it wicks above $80,900 into $81,050 and then falls back inside the range, treat that as a sweep, not a breakout. 3. Only enter after the failed breakout candle closes back below the range high. 4. A short from $80,850 with a stop at $81,120 risks about $270. 5. If the range midpoint is $80,350 and the opposite side is $79,850, the setup offers enough room to manage partials professionally."
        ),
        (
            "Execution Playbook: Trend Pullback Long",
            "If 4H structure is bullish and BTC has already broken to $82,000, the better trade is often the pullback, not the chase. "
            "How to do it: 1. Mark the breakout base around $81,350. 2. Wait for price to revisit that zone. 3. Look for a 5m or 15m hold with buyers stepping in. 4. Enter only after the market proves support is being defended. 5. If entry is $81,420 and stop is $81,120, risk is $300. If the next obvious liquidity is $82,300, the reward is about $880, which keeps the trade efficient."
        ),
        (
            "Execution Playbook: Counter-Trend Fade",
            "Counter-trend trades need stricter rules because the larger flow is against you. "
            "What to do: 1. Only fade an extension after a sweep into a known level, for example BTC pushing from $80,600 into $81,450 resistance. 2. Wait for rejection and loss of momentum. 3. Enter smaller than normal, such as 50-70% of usual risk. 4. Keep the stop tight above the rejection high. 5. If the reaction does not start quickly, exit early, because slow counter-trend trades often become squeezes."
        ),
        (
            "Execution Playbook: Missed Entry Recovery",
            "If the planned long entry was $79,900 and price runs straight to $80,700 without you, the answer is not to chase. "
            "How to do it instead: 1. Accept that the original entry is gone. 2. Rebuild the plan around the next pullback level, maybe $80,250-$80,350. 3. Only re-enter if structure still supports the idea. 4. If no clean retest comes, skip the trade. Missing one move hurts much less than forcing a bad fill with $300-$500 of extra risk."
        ),
        (
            "Execution Playbook: Move to Breakeven",
            "Breakeven should be earned, not given automatically after two green candles. "
            "A cleaner method is: 1. Risk $400 to make at least $800. 2. After price reaches roughly 1R or takes the first liquidity pocket, reduce part of the trade. 3. Then move the stop to entry only if the market has already left the zone with strong acceptance. 4. If price is still chopping near entry, keeping the original stop for a bit longer is often smarter than getting tagged at breakeven before the real move."
        ),
        (
            "Execution Playbook: TP1 and Runner Management",
            "Imagine a long from $80,100 with a stop at $79,700, so 1R is $400. "
            "How to manage it: 1. If TP1 sits at $80,500, close 40-50% there. 2. Move the stop tighter only after the market accepts above that first target. 3. Let the runner target the larger level, maybe $81,000 or $81,300. 4. This keeps realized money in the account while still allowing one piece of the position to catch the full extension."
        ),
        (
            "Execution Playbook: No-Trade Filter",
            "A strong trader knows when not to trade. "
            "What to do: 1. Skip if BTC is in a $150-$220 15m chop with no clean level break. 2. Skip if the stop would need $700 but the nearest target is only $500 away. 3. Skip if major news is 10-15 minutes away. 4. Skip if you already hit the daily loss cap. The best no-trade decision often protects more capital than the best entry signal makes."
        ),
        (
            "Execution Playbook: ATR-Based Stop Placement",
            "If 15m ATR is about $280, stops tighter than $80-$120 may simply be noise bait. "
            "A better process is: 1. Start from the invalidation level on structure. 2. Check whether the stop is at least realistic versus current ATR. 3. If the structure stop is $260 away and ATR is $280, that makes sense. 4. If the structure stop is only $70 while ATR is $280, either widen the stop properly and reduce size or skip the setup."
        ),
        (
            "Execution Playbook: Session Open Attack",
            "London and New York opens often produce the first real liquidity sweep of the day. "
            "How to use it: 1. Mark Asia high and Asia low. 2. Let the session open take one side first. 3. If price sweeps Asia high by $120-$200 and then re-enters the range, look for the reversal back toward range midpoint. 4. If instead price sweeps and holds, shift to continuation logic. The point is to react to what the session does, not to predict before the move starts."
        ),
        (
            "Execution Playbook: News Hour Discipline",
            "If CPI or FOMC is due in 10 minutes, the best trade may be no trade. "
            "What to do: 1. Flatten weak positions ahead of the event. 2. Do not open a fresh trade just because the chart still looks clean. 3. Wait for the first violent expansion to finish. 4. Rebuild the plan only after a new high or low is formed and the reclaim or retest becomes visible. Preserving mental capital around news is part of professional execution."
        ),
        (
            "Execution Playbook: Funding Bias Use",
            "Funding should guide context, not trigger entries by itself. "
            "If funding is strongly positive, for example +0.030% to +0.050%, longs may be crowded. What to do: 1. Stop thinking breakout first. 2. Look for where an upside squeeze could fail or where a downside flush could wash out late longs. 3. Only if price reclaims after that flush does a long become attractive again. Funding is most useful when it changes how you frame risk, not when it replaces structure."
        ),
        (
            "Execution Playbook: Size Reduction After Losses",
            "After two full losses, the next job is stability. "
            "A professional reset looks like this: 1. If normal risk is 1.0%, cut to 0.5%. 2. Take only A-quality setups for the next 2-3 trades. 3. If the market is still messy, stop for the day. 4. Only return to full size after the process is back to normal. Cutting risk after losses protects both the account and your decision quality."
        ),
        (
            "Execution Playbook: Daily Plan Before Trading",
            "Before the session opens, write the plan in simple numbers. "
            "Example: 1. Bullish only above $81,150. 2. Bearish only below $80,420. 3. Maximum risk today $200 on a $20,000 account, which is 1.0%. 4. Maximum two full-risk attempts. 5. No new positions after major U.S. data. A written plan makes emotional decisions much harder to justify once the market speeds up."
        ),
        (
            "Execution Playbook: Choosing Between Two Setups",
            "If two trades appear at the same time, pick the one with cleaner asymmetry. "
            "For example, setup A risks $260 to make $520 and is with trend, while setup B risks $420 to make $630 and is counter-trend. Even if both can work, the first one is usually superior because the structure is cleaner, the stop is tighter, and the trend is helping you instead of fighting you."
        ),
        (
            "Execution Playbook: Trade Journal Entry",
            "After every closed trade, write down five things: entry, stop, target, reason for entry, reason for exit. "
            "Then add one number for quality from 1 to 5. Example: long at $80,200, stop $79,820, TP $81,050, reason was 15m reclaim after sweep, exit was runner stopped at breakeven, quality 4 out of 5. A journal like this turns random memory into reusable data."
        ),
        (
            "Execution Playbook: When to Hold the Runner",
            "The runner should stay open only when the market keeps giving evidence. "
            "What to do: 1. After TP1 and TP2, keep the last piece only if higher highs or lower lows continue printing cleanly. 2. If momentum stalls under a major level like $82,000 and volume dries up, pay yourself instead of hoping. 3. The runner is for expansion, not for charity."
        ),
        (
            "Execution Playbook: Retest Entry",
            "A retest is valid only if the market accepts the new side of the level. "
            "Example: BTC breaks above $80,750 and later pulls back to $80,780. What to do: 1. Watch whether that old resistance now holds as support. 2. Enter only if the pullback stabilizes and buyers defend it. 3. Place the stop under the retest low, not at a random round number. 4. If the level breaks immediately, the retest failed and there is no long."
        ),
        (
            "Execution Playbook: Tightening a Wide Stop",
            "If the correct stop is too wide, do not force the same size. "
            "Suppose the proper stop is $900 away and your risk limit is $100. That means the position notional can only be about $11,111 because 0.9% of that size equals the allowed loss. If that size is too small to be worth the trade, the solution is to pass, not to move the stop to an unrealistic place just to trade bigger."
        ),
        (
            "Execution Playbook: Rejection Candle Confirmation",
            "One wick is not enough by itself. "
            "A better method is: 1. Let resistance at $81,400 get tested. 2. If price wicks to $81,520 and closes back below $81,350, note the rejection. 3. Enter only after the next candle fails to reclaim the high. 4. Stop goes above the wick high. 5. This extra confirmation removes many weak rejection trades that fail on the next candle."
        ),
        (
            "Execution Playbook: Protecting a Green Day",
            "If the day is already up +2.0R, the job changes from making more money to keeping the day clean. "
            "What to do: 1. Reduce risk per new trade, for example from 1.0% to 0.5%. 2. Take only the best setup, not every setup. 3. If one reduced-risk trade loses, consider ending the day. Professionals protect good days aggressively because consistency matters more than squeezing one extra trade."
        ),
        (
            "Execution Playbook: Stop Trading After Emotional Trigger",
            "If a missed move or a stop-out makes you want to win it back immediately, that is already useful information. "
            "What to do: 1. Pause for 10-15 minutes. 2. Re-mark the levels from scratch. 3. Ask whether the next trade idea existed before the frustration. 4. If not, it is probably emotional and should be skipped. Many avoidable losses happen not from bad charts, but from trading while mentally tilted."
        ),
        (
            "Execution Playbook: Liquidity Target Mapping",
            "Before entering, know where price is most likely to travel if you are right. "
            "Example: long at $79,950 after a reclaim. If equal highs sit at $80,620 and a larger short-liquidity pocket sits at $81,280, your plan can be: 1. TP1 near $80,620. 2. Leave the runner for $81,280. 3. If price cannot even reach the first liquidity pocket, that is already information that the move is weaker than expected."
        ),
        (
            "Execution Playbook: Weekly Risk Ceiling",
            "Daily control is not enough if the whole week is going badly. "
            "A professional weekly limit can be 4-6R. On a $10,000 account risking 1.0% per trade, 5R is about $500. If the week reaches that loss, reduce size sharply or stop until the next week starts. Weekly limits prevent a bad Tuesday from becoming a destructive Friday."
        ),
    ]
    return [_education_post(topic, body) for topic, body in playbooks]


def _technical_topic_posts(module_title, topic, lesson, example_one, example_two, takeaway):
    return [
        _education_post(
            f"{module_title} — {topic} | Example 1",
            f"{lesson} Example 1: {example_one} {takeaway}",
        ),
        _education_post(
            f"{module_title} — {topic} | Example 2",
            f"{lesson} Example 2: {example_two} {takeaway}",
        ),
    ]


def _technical_topic_content(module_title, topic):
    topic_lower = str(topic or "").lower()
    custom = [
        (
            ["fvg", "fair value gap"],
            (
                "Fair Value Gap is a three-candle imbalance where price moved so fast that inefficient space was left behind.",
                "BTC prints a strong displacement from 79,800 to 80,420 and leaves a gap between the first candle high and the third candle low. If price later comes back into that gap and holds, the FVG can act like a continuation demand zone.",
                "ETH sells off hard, leaves a bearish FVG overhead, then rallies back into that gap but rejects before filling the whole area. That kind of FVG failure can become a short continuation entry.",
                "The key rule is that FVG is not a blind entry. It works best when it aligns with structure, liquidity, and trend.",
            ),
        ),
        (
            ["order block", "(ob)", " ob"],
            (
                "An order block is usually the last opposing candle or small base before the displacement that actually broke structure.",
                "BTC breaks structure upward and the last bearish candle before the impulse becomes the bullish order block. When price returns there and shows acceptance, traders often use it as a precise long zone instead of chasing the breakout.",
                "ETH breaks down from a clear swing low, and the last bullish candle before that displacement becomes the bearish order block. If price retests that candle body and fails to reclaim it, the order block has done its job.",
                "Good order blocks are linked to real displacement and structure shift. Without that, it is just another candle zone.",
            ),
        ),
        (
            ["bos", "break of structure"],
            (
                "Break of Structure matters when price takes out a meaningful swing and closes through it with conviction.",
                "BTC forms higher lows, then breaks the last clear lower high at 80,600 with a displacement candle. That is a bullish BOS because structure advanced, not just because price poked above a minor wick.",
                "ETH loses a key higher low on 1H and cannot reclaim it on the retest. That is a bearish BOS because the prior supportive structure actually failed.",
                "A proper BOS usually changes what side gets priority next. That is why it matters for bias, not only for labels.",
            ),
        ),
        (
            ["choch", "change of character"],
            (
                "Change of Character is often the first meaningful sign that the old trend is losing control.",
                "BTC has been printing lower highs and lower lows, but then it suddenly breaks the last lower high and holds above it. That first break can be read as CHoCH before the full bullish structure is confirmed.",
                "ETH trends up cleanly, then fails to make a new higher high and loses the last higher low. That shift is often the first bearish CHoCH before a larger breakdown follows.",
                "CHoCH is earlier than full confirmation, so it is useful, but it should be respected together with liquidity and confirmation, not alone.",
            ),
        ),
        (
            ["liquidity", "stop hunt", "liquidity grab"],
            (
                "Liquidity is where obvious stops or breakout orders tend to sit, and the market often targets it before the real move begins.",
                "BTC builds equal lows at 79,950, sweeps them to 79,880, and then reclaims. The sweep is not the long entry by itself; the reclaim is the part that proves the sell-side liquidity was likely taken.",
                "ETH builds equal highs under 3,420, runs through them by a few dollars, and then closes back below. That is a classic buy-side stop hunt, and the better short usually comes after the failed hold.",
                "Think of liquidity as a magnet first and an entry trigger only after price proves what it wants to do there.",
            ),
        ),
        (
            ["rsi"],
            (
                "RSI measures momentum, but context decides whether that momentum matters.",
                "BTC can stay above RSI 70 for several candles during a strong trend. That alone does not make it a short if structure still prints higher highs and higher lows.",
                "ETH can print bullish divergence into a support zone while RSI makes a higher low against a lower price low. That becomes useful only if price also reclaims the level.",
                "RSI is strongest as a supporting clue, not as a stand-alone reason to enter.",
            ),
        ),
        (
            ["macd"],
            (
                "MACD is helpful for momentum shifts, but it is still a lagging tool that should confirm price, not replace it.",
                "BTC reclaims a level while MACD histogram turns from deeply negative toward neutral. That supports the idea that bearish momentum is fading.",
                "ETH prints a MACD bullish cross, but price is still trapped under resistance and cannot close above it. In that case MACD is not enough to justify the long.",
                "When MACD agrees with structure and liquidity, it adds weight. When it fights the chart, the chart wins.",
            ),
        ),
        (
            ["moving averages", "ema", "sma"],
            (
                "Moving averages are best used as trend filters and dynamic support or resistance.",
                "BTC pulls back into a rising 20 EMA while 4H structure stays bullish. That kind of touch is often more useful than buying a random green candle far from the average.",
                "ETH trades below a falling 200 SMA, and every rally into the 50 EMA gets sold. That tells you the trend filter still favors short-side setups.",
                "The average is context, not permission. Price still needs a trigger at the level.",
            ),
        ),
        (
            ["atr"],
            (
                "ATR tells you whether your stop is realistic relative to current volatility.",
                "If BTC 15m ATR is about $280 and you place a $70 stop inside that noise, getting stopped out does not necessarily mean the idea was wrong; the stop was simply too small for the environment.",
                "If ETH 1H ATR is 1.5% and your target is only 0.4%, the setup may not offer enough room to justify the trade.",
                "ATR protects traders from choosing stops and targets that the current market regime is unlikely to respect.",
            ),
        ),
        (
            ["vwap", "volume profile"],
            (
                "VWAP and volume profile show where business was done and where price may react around fair value.",
                "BTC can reclaim VWAP after a sweep and use it as intraday support. That often tells you buyers are regaining control around the average transaction area.",
                "ETH can stall inside a high-volume node because that area previously hosted a lot of two-way trade. Until it escapes, trend continuation may remain slow.",
                "These tools work best when they support a structure story, not when they become the whole story.",
            ),
        ),
        (
            ["fibonacci", "fib"],
            (
                "Fibonacci is most useful when applied to obvious swings that already matter structurally.",
                "BTC rallies from 79,400 to 80,900, then pulls back into the 0.5 to 0.618 zone while that same area overlaps with an old breakout level. That confluence is what makes the retracement interesting.",
                "ETH breaks down, then retests near the 0.618 pullback where a bearish order block sits. That overlap creates a more believable short than the fib level alone.",
                "Fib becomes valuable when it agrees with price memory, liquidity, or trend structure.",
            ),
        ),
        (
            ["support", "resistance"],
            (
                "Support and resistance matter because they show where buyers or sellers previously defended price.",
                "BTC can bounce three times from 79,800 and prove that level matters. The more important question then becomes whether the next test holds or sweeps the stops below it.",
                "ETH can reject several times from 3,420, which turns that price into resistance. If the market later closes above and retests it successfully, resistance can flip into support.",
                "A level is useful when it changes behavior, not just because a line exists on the chart.",
            ),
        ),
        (
            ["breakout", "fakeout"],
            (
                "Breakouts that hold are different from fakeouts that simply grab liquidity and fail.",
                "BTC closes above 81,000 and then retests it as support. That is a more trustworthy breakout than a wick above 81,000 that falls right back inside the range.",
                "ETH pushes through range highs, attracts breakout buyers, then loses the level on the next candle. That is the kind of fakeout that often creates the better short after the failure.",
                "The hold or failure after the break matters more than the first touch through the line.",
            ),
        ),
        (
            ["engulfing", "doji", "hammer", "shooting star"],
            (
                "Candlestick patterns only matter when they appear in useful context.",
                "BTC can print a hammer after sweeping a known low. That is much more meaningful than a hammer in the middle of nowhere.",
                "ETH can form a bearish engulfing pattern at a premium retracement or resistance retest. There the pattern is confirming rejection, not inventing it.",
                "Pattern plus level plus context is what makes the read professional.",
            ),
        ),
        (
            ["psychology", "fear", "greed", "revenge", "overtrading", "bias"],
            (
                "Psychology topics are practical because most trader mistakes come from behavior, not from missing one more indicator.",
                "After a BTC stop-out, a trader immediately flips short without a real setup just to get the loss back. That is revenge trading, and the market does not care that the previous trade hurt.",
                "After a winning ETH trade, greed can make the trader oversize the next setup or hold the runner with no evidence left. That turns a good day into a messy one.",
                "The fix is routine, size control, and written process, not just motivation.",
            ),
        ),
        (
            ["margin", "liquidation", "leverage"],
            (
                "Leverage changes margin efficiency, but liquidation risk turns execution mistakes into much bigger problems.",
                "A BTC long with a structurally correct stop can still be dangerous if the liquidation price sits too close because leverage is excessive.",
                "An ETH futures trade may look small in margin terms, but if the real stop-based risk is not controlled, leverage is just making the mistake faster.",
                "Risk should always be built from invalidation first and leverage second, never the other way around.",
            ),
        ),
    ]

    for keys, content in custom:
        if any(key in topic_lower for key in keys):
            return content

    if "մոդուլ 4" in module_title.lower():
        return (
            "This topic belongs to trader psychology, so the useful question is always how the behavior shows up in real execution.",
            "On BTC, the concept usually becomes visible when the trader changes the plan because of fear, greed, tilt, or impatience rather than because the chart truly changed.",
            "On ETH, the same concept often appears when the trader forces extra trades during chop or refuses to accept a planned stop because of emotion.",
            "The practical edge comes from identifying the behavior early and giving it a process-based correction.",
        )
    if "մոդուլ 9" in module_title.lower():
        return (
            "This topic is about tools, so the main lesson is that every script or indicator should speed up observation without replacing judgement.",
            "On BTC, a tool can help surface levels, structure, or alerts faster, but the trader still has to decide whether the context is clean enough to act.",
            "On ETH, the same script can become noise if it plots too many levels and makes the chart harder to read than the raw price itself.",
            "Good tool usage means faster clarity, not more clutter.",
        )
    if "մոդուլ 10" in module_title.lower():
        return (
            "This topic is about live trading reality, so the useful angle is always risk, execution, and operational safety.",
            "On BTC, the lesson often shows up in how orders are filled, how stops are respected, and whether the trader follows the plan under pressure.",
            "On ETH, the same lesson often appears in leverage decisions, margin awareness, or whether the trader is using the right product for the objective.",
            "The goal is not just a good idea, but safe and repeatable execution.",
        )
    if "մոդուլ 7" in module_title.lower() or "մոդուլ 8" in module_title.lower():
        return (
            "This topic lives inside Smart Money Concepts, so the main task is to connect liquidity, structure, and displacement instead of treating the label like magic.",
            "On BTC, the clean example is usually a sweep, structure shift, or retracement into a marked zone where the move actually began.",
            "On ETH, the same concept often becomes clearer when a premium or discount area lines up with a rejection, reclaim, or displacement candle.",
            "The concept becomes useful only when it changes the plan, not when it is just another annotation on the chart.",
        )
    if "մոդուլ 5" in module_title.lower():
        return (
            "This topic is about indicators, so the real edge is learning what the tool confirms and what it cannot confirm by itself.",
            "On BTC, an indicator is most helpful when it agrees with a structure or liquidity read that already makes sense on the chart.",
            "On ETH, the same indicator can become misleading if it is used alone while price is still stuck inside resistance, support, or range noise.",
            "Indicators are strongest as filters and confirmations, not as blind entry buttons.",
        )
    if "մոդուլ 6" in module_title.lower():
        return (
            "This topic is about entries and trade construction, so the main question is always where the invalidation sits and whether the reward justifies the risk.",
            "On BTC, a good example is waiting for the cleaner retracement or retest instead of forcing the trade from the middle of the move.",
            "On ETH, the same idea often means using confluence so the entry is tighter, the stop is more logical, and the target has real room to travel.",
            "If the entry style improves precision, the whole trade improves with it.",
        )
    if "մոդուլ 3" in module_title.lower():
        return (
            "This topic is about trend and price action, so the point is to read how price is moving before deciding what strategy fits.",
            "On BTC, the concept becomes visible when you compare impulsive candles to corrective pullbacks and see whether the market is trending or just ranging.",
            "On ETH, the same read often helps decide whether you should follow continuation or wait for a reversal signal after exhaustion.",
            "Price action is useful when it changes what kind of trade you take, not just how you describe the chart afterward.",
        )
    return (
        "This topic is part of technical chart reading, so the goal is to see how the concept appears in real structure instead of memorizing the term only.",
        "On BTC, the best example usually shows up around a clear level, trend phase, or liquidity event where the concept can change entry timing.",
        "On ETH, the same concept often looks cleaner because the move is smaller in dollars, but the logic is still the same: context first, trigger second.",
        "Always ask how this concept would improve your entry, stop, target, or no-trade decision.",
    )


def _technical_curriculum_posts():
    curriculum = [
        ("✅ Մոդուլ 2․ Մոմային անալիզ և շուկայի կառուցվածք", [
            "11. Candlestick-ի կառուցվածքը և տրամաբանությունը",
            "12. Bullish/Bearish patterns – Engulfing, Doji, Hammer, Shooting Star",
            "13. Թայմֆրեյմերի աշխատանք – M1-ից մինչև 1W",
            "14. Support և Resistance՝ հորիզոնական և դինամիկ",
            "15. Breakouts և Fakeouts",
            "16. Swing High / Swing Low",
            "17. Շուկայի կառուցվածք – HH, HL, LH, LL",
            "18. Կոնսոլիդացիա / ռենջ",
            "19. Գործնական – նշում իրական գրաֆիկների վրա",
            "20. BTC/ETH օրինակի անալիզ",
        ]),
        ("✅ Մոդուլ 3․ Թրենդ, գնի շարժ և Price Action", [
            "21. Թրենդի տեսակներ – աճ, անկում, հորիզոնական",
            "22. Թրենդի փուլեր – Accumulation, Expansion, Distribution",
            "23. Price Action-ի հիմունքներ",
            "24. Impulse և Correction շարժեր",
            "25. Հակառակ տենդենցի ձևեր – Double Top/Bottom, Head & Shoulders",
            "26. Շարունակականություն՝ Flag, Pennant, Triangle",
            "27. Ծավալ և թրենդի ուժ",
            "28. Գործնական վարժանքներ",
            "29. Թրենդի փոփոխության նշաններ",
            "30. Price Action ստրատեգիայի կառուցում",
        ]),
        ("✅ Մոդուլ 4․ Թրեյդերի հոգեբանություն", [
            "31. Վախ և ագահություն",
            "32. Սխալներ՝ Revenge Trading, Overtrading",
            "33. Cognitive biases՝ Confirmation bias, Loss aversion",
            "34. Ռիսկի հանդուրժողականություն և էմոցիոնալ վերահսկում",
            "35. Trading Journal – գրանցում, վերլուծություն",
            "36. Trading ռեժիմներ և ռուտինա",
            "37. Volatile շուկաների հոգեբանական մարտահրավերներ",
            "38. Drawdown-ների հետ աշխատանք",
            "39. Սիմուլյացիոն վարժանքներ",
            "40. Գտիր քո թրեյդերական տիպը",
        ]),
        ("✅ Մոդուլ 5․ Տեխնիկական ինդիկատորներ", [
            "41. Moving Averages (EMA/SMA) – 20, 50, 200",
            "42. RSI – Relative Strength Index",
            "43. MACD",
            "44. Bollinger Bands",
            "45. Stochastic Oscillator",
            "46. ATR – Average True Range",
            "47. Volume Profile, VWAP",
            "48. Դիվերգենցիա",
            "49. Ինդիկատորների համադրություն",
            "50. Ինդիկատորով ստրատեգիայի կառուցում",
        ]),
        ("✅ Մոդուլ 6․ Fibonacci և մուտքի տեխնիկաներ", [
            "51. Fibonacci Retracement/Extension",
            "52. Կոնֆլյուենցիայով զոնաների հայտնաբերում",
            "53. Մուտքի ձևեր՝ Breakout Entry, Retest Entry",
            "54. Risk-Reward Ratio",
            "55. SL և TP-ի ճիշտ տեղադրում",
            "56. Մասնակի profit-ի ֆիքսում",
            "57. Swing",
            "58. Scaling ծանոթացում",
            "59. Trade Setup ձևաչափեր",
            "60. Գործնական վարժանք – Fibonacci-ի կիրառում",
        ]),
        ("✅ Մոդուլ 7․ Smart Money Concepts (SMC)", [
            "61. Ի՞նչ է Smart Money – ինստիտուցիոնալ մտածելակերպ",
            "62. Liquidity – Buy-side / Sell-side",
            "63. Liquidity Grabs / Stop Hunt",
            "64. BOS – Break of Structure",
            "65. CHoCH – Change of Character",
            "66. Order Blocks (OB)",
            "67. Fair Value Gaps (FVG)",
            "68. Inducement – ներքին լիկվիդության մանիպուլյացիա",
            "69. Premium / Discount Zones",
            "70. BTC/ETH օրինակներով Smart Money վերլուծություն",
        ]),
        ("✅ Մոդուլ 8․ SMC ստրատեգիաներ", [
            "71. Order Block Entry Strategy",
            "72. Liquidity Sweep Strategy",
            "73. FVG + OB Combo",
            "74. Asian Range + London Breakout",
            "75. Supply & Demand Zones",
            "76. SMC Trading Plan-ի կառուցում",
            "77. High R:R Setup-ների ձևավորում",
            "78. Smart Risk Management",
            "79. SMC vs Indicator Analysis",
            "80. Trading Journal SMC-ի համար",
        ]),
        ("✅ Մոդուլ 9․ Smart Money ինդիկատորներ", [
            "81. LuxAlgo Smart Money Concepts",
            "82. Smart Money Indicator by Quantlab",
            "83. OB & BOS Auto Plot",
            "84. FVG Detector",
            "85. Liquidity Maps – Coinglass, Hyblock",
            "86. Volume Imbalance Tools",
            "87. ICT Concepts Indicator (Free)",
            "88. Alerts – BOS, OB, FVG",
            "89. Սքրիպտեր՝ ռիսկի կառավարման համար",
            "90. TradingView գործիքների հավաքածու",
        ]),
        ("✅ Մոդուլ 10․ Իրական առևտուր և անվտանգություն", [
            "91. Ճշգրիտ trade execution",
            "92. Spot և Futures առևտուր",
            "93. Լեվերիջի ճիշտ օգտագործում",
            "94. Margin և Liquidation",
            "95. Դրամապանակներ՝ Cold vs Hot",
            "96. KYC, AML, կարգավորող պահանջներ",
            "97. Բոտեր և Copy-trading",
            "98. Ամենօրյա թրեյդինգային պլան",
            "99. Եզրափակիչ ստուգում",
            "100. Դիպլոմ + հետադարձ կապ",
        ]),
    ]

    posts = []
    for module_title, topics in curriculum:
        for topic in topics:
            lesson, example_one, example_two, takeaway = _technical_topic_content(module_title, topic)
            posts.extend(_technical_topic_posts(module_title, topic, lesson, example_one, example_two, takeaway))
    return posts


def build_professional_member_education_posts():
    groups = [
        _technical_curriculum_posts(),
        _actionable_playbook_posts(),
        _session_and_news_posts(),
        _structure_and_liquidity_posts(),
        _execution_posts(),
        _journal_posts(),
        _rr_and_filter_posts(),
        _scale_out_posts(),
        _drawdown_posts(),
        _expectancy_posts(),
        _leverage_posts(),
        _daily_limit_posts(),
        _risk_sizing_posts(),
    ]
    return _interleave_post_groups(groups)


PROFESSIONAL_MEMBER_EDUCATION_POSTS = build_professional_member_education_posts()
assert len(PROFESSIONAL_MEMBER_EDUCATION_POSTS) >= 150
