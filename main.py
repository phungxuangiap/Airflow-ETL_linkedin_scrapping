"""
LinkedIn Jobs ETL Pipeline - Docker Entry Point
Supports Bronze, Silver, and Gold layer execution via CLI
"""
import argparse
import sys
from datetime import datetime

from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_bronze(load_date: str, step: str = 'all'):
    logger.info("=== Starting Bronze Layer ===")
    logger.info(f"Load date: {load_date}, Step: {step}")

    if step == 'process_to_staging':
        from src.jobs.bronze.extract_and_load_bronze import process_to_staging
        result = process_to_staging(load_date=load_date)
    elif step == 'promote':
        from src.jobs.bronze.extract_and_load_bronze import promote_staging_to_bronze
        result = promote_staging_to_bronze(load_date=load_date)
    elif step == 'all':
        from src.jobs.bronze.extract_and_load_bronze import run
        result = run(load_date=load_date)
    else:
        raise ValueError(f"Invalid Bronze step: {step}. Must be 'process_to_staging', 'promote', or 'all'")

    logger.info("=== Bronze Layer Completed ===")
    logger.info(f"Result: {result}")

    return result


def run_silver(load_date: str, step: str = 'all'):
    logger.info("=== Starting Silver Layer ===")
    logger.info(f"Load date: {load_date}, Step: {step}")

    if step == 'process_to_staging':
        from src.jobs.silver.transform_and_load_silver import process_to_staging
        result = process_to_staging(load_date=load_date)
    elif step == 'promote':
        from src.jobs.silver.transform_and_load_silver import promote_staging_to_silver
        result = promote_staging_to_silver(load_date=load_date)
    elif step == 'all':
        from src.jobs.silver.transform_and_load_silver import run
        result = run(load_date=load_date)
    else:
        raise ValueError(f"Invalid Silver step: {step}. Must be 'process_to_staging', 'promote', or 'all'")

    logger.info("=== Silver Layer Completed ===")
    logger.info(f"Result: {result}")

    return result


def run_staging(load_date: str, step: str = 'clean'):
    logger.info("=== Starting Staging Maintenance ===")
    logger.info(f"Load date: {load_date}, Step: {step}")

    if step == 'clean':
        from src.jobs.staging.cleanup import run
        result = run(load_date=load_date)
    else:
        raise ValueError(f"Invalid Staging step: {step}. Must be 'clean'")

    logger.info("=== Staging Maintenance Completed ===")
    logger.info(f"Result: {result}")

    return result


def run_gold(load_date: str, step: str = 'all'):
    """
    Execute Gold layer: Build star schema and analytics tables

    Args:
        load_date: Date to process
        step: Which step to run ('dimensions', 'fact', 'load', or 'all')
    """
    logger.info(f"=== Starting Gold Layer ===")
    logger.info(f"Load date: {load_date}, Step: {step}")

    if step == 'dimensions':
        from src.jobs.gold.build_dimensions import run
        result = run(load_date=load_date)
        logger.info(f"Dimensions built: {result}")

    elif step == 'dimensions_to_staging':
        from src.jobs.gold.build_dimensions import process_dimensions_to_staging
        result = process_dimensions_to_staging(load_date=load_date)
        logger.info(f"Dimensions staged: {result}")

    elif step == 'promote_dimensions':
        from src.jobs.gold.build_dimensions import promote_dimensions_to_gold
        result = promote_dimensions_to_gold(load_date=load_date)
        logger.info(f"Dimensions promoted: {result}")

    elif step == 'fact':
        from src.jobs.gold.build_fact_table import run
        result = run(load_date=load_date)
        logger.info(f"Fact table built: {result}")

    elif step == 'fact_to_staging':
        from src.jobs.gold.build_fact_table import process_fact_to_staging
        result = process_fact_to_staging(load_date=load_date)
        logger.info(f"Fact tables staged: {result}")

    elif step == 'promote_fact':
        from src.jobs.gold.build_fact_table import promote_fact_to_gold
        result = promote_fact_to_gold(load_date=load_date)
        logger.info(f"Fact tables promoted: {result}")

    elif step == 'load':
        from src.jobs.gold.load_star_schema import run
        result = run(load_date=load_date)
        logger.info(f"Star schema loaded: {result}")

    elif step == 'all':
        # Run all steps sequentially
        from src.jobs.gold.build_dimensions import run as build_dims
        from src.jobs.gold.build_fact_table import run as build_fact
        from src.jobs.gold.load_star_schema import run as load_schema

        dims_result = build_dims(load_date=load_date)
        logger.info(f"Dimensions built: {dims_result}")

        fact_result = build_fact(load_date=load_date)
        logger.info(f"Fact table built: {fact_result}")

        load_result = load_schema(load_date=load_date)
        logger.info(f"Star schema loaded: {load_result}")

        result = {
            'dimensions': dims_result,
            'fact': fact_result,
            'load': load_result
        }
    else:
        raise ValueError(
            f"Invalid Gold step: {step}. Must be 'dimensions', 'dimensions_to_staging', "
            "'promote_dimensions', 'fact', 'fact_to_staging', 'promote_fact', 'load', or 'all'"
        )

    logger.info(f"=== Gold Layer Completed ===")
    logger.info(f"Result: {result}")

    return result


def main():
    parser = argparse.ArgumentParser(
        description="LinkedIn Jobs ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --layer bronze --load-date 2026-05-21
  python main.py --layer silver --load-date 2026-05-21
  python main.py --layer gold --load-date 2026-05-21
        """
    )

    parser.add_argument(
        '--layer',
        type=str,
        required=True,
        choices=['bronze', 'silver', 'gold', 'staging'],
        help='ETL layer to execute (bronze, silver, gold, or staging)'
    )

    parser.add_argument(
        '--load-date',
        type=str,
        default=datetime.now().strftime('%Y-%m-%d'),
        help='Load date in YYYY-MM-DD format (default: today)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )

    parser.add_argument(
        '--step',
        type=str,
        default='all',
        choices=[
            'process_to_staging',
            'promote',
            'dimensions',
            'dimensions_to_staging',
            'promote_dimensions',
            'fact',
            'fact_to_staging',
            'promote_fact',
            'load',
            'clean',
            'all'
        ],
        help='Layer step to execute (default: all)'
    )

    args = parser.parse_args()

    # Validate load_date format
    try:
        datetime.strptime(args.load_date, '%Y-%m-%d')
    except ValueError:
        logger.error(f"Invalid load_date format: {args.load_date}. Expected YYYY-MM-DD")
        sys.exit(1)

    # Execute the appropriate layer
    try:
        logger.info(f"Starting ETL Pipeline - Layer: {args.layer.upper()}")

        if args.layer == 'bronze':
            result = run_bronze(args.load_date, step=args.step)
        elif args.layer == 'staging':
            result = run_staging(args.load_date, step=args.step)
        elif args.layer == 'silver':
            result = run_silver(args.load_date, step=args.step)
        elif args.layer == 'gold':
            result = run_gold(args.load_date, step=args.step)

        logger.info(f"✅ Pipeline completed successfully: {args.layer}")
        logger.info(f"Final result: {result}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
