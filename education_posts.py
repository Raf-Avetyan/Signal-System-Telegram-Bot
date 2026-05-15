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


def build_professional_member_education_posts():
    groups = [
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
