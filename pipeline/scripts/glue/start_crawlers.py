import boto3
import re
from tap import Tap


class Args(Tap):
    region: str = ""  # The AWS region.
    crawler_expression: str = ""  # The regular expression of tables to delete.
    dryrun: bool = False  # Whether to run in a silent mode.
    force: bool = False  # Whether to skip the prompt.


def start_crawlers(args: Args):
    """Start crawlers."""
    client = boto3.client('glue', region_name=args.region)

    # 1k is arbtirary.
    crawlers_response = client.list_crawlers(MaxResults=1000)
    crawler_names = crawlers_response['CrawlerNames']

    if len(crawler_names) == 1000:
        raise Exception("Too many crawlers (>1k).  Change the code.")

    crawler_names = list(filter(lambda x: re.search(args.crawler_expression, x), crawler_names))

    if len(crawler_names) == 0:
        print("\nNo crawlers.")
        return

    print(f"Crawlers to run ({len(crawler_names)}):")
    for crawler_name in crawler_names:
        print(f"- {crawler_name}")

    if args.dryrun:
        print("\nNot running because `--dryrun` as specified")
        return

    if not args.force:
        user_input = input("\nEnter 'yes' to continue: ")
        if user_input != 'yes':
            print("Not deleting.")
            return

    for crawler_name in crawler_names:
        print(f"Starting {crawler_name}")
        client.start_crawler(
            Name=crawler_name
        )
    print("\nFinished.")


if __name__ == "__main__":
    args = Args().parse_args()
    start_crawlers(args)
