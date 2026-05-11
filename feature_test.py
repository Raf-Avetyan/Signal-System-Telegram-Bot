import argparse

from bot import PonchBot


def main():
    parser = argparse.ArgumentParser(description="Trigger feature test posts from terminal.")
    parser.add_argument(
        "action",
        choices=["all", "snapshot", "liqmap", "education"],
        help="Which feature test to run.",
    )
    parser.add_argument(
        "--side",
        choices=["LONG", "SHORT"],
        default="LONG",
        help="Side used for demo signal snapshot tests.",
    )
    args = parser.parse_args()

    bot = PonchBot(quiet_init=True)
    bot.is_booting = False

    demo_sig = bot._build_demo_signal(side=args.side)

    if args.action in {"all", "snapshot"}:
        bot._send_active_trade_snapshot(demo_sig)
        print("snapshot test sent")

    if args.action in {"all", "liqmap"}:
        bot._send_liquidation_map_post()
        print("liquidation map test sent")

    if args.action in {"all", "education"}:
        bot._send_member_education_post()
        print("education test sent")


if __name__ == "__main__":
    main()
