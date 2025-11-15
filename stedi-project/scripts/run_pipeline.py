from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_full_pipeline():
    """
    Execute the complete STEDI data pipeline
    """
    spark = SparkSession.builder \
        .appName("STEDI-Full-Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Stage 1: Landing Zone
        logger.info("Stage 1: Loading Landing Zone data...")
        exec(open("/home/glue_user/stedi-project/scripts/landing_zone.py").read())
        logger.info("✓ Landing Zone complete")
        
        # Stage 2: Trusted Zone
        logger.info("Stage 2: Building Trusted Zone...")
        exec(open("/home/glue_user/stedi-project/scripts/trusted_zone.py").read())
        logger.info("✓ Trusted Zone complete")
        
        # Stage 3: Curated Zone
        logger.info("Stage 3: Creating Curated Zone...")
        exec(open("/home/glue_user/stedi-project/scripts/curated_zone.py").read())
        logger.info("✓ Curated Zone complete")
        
        # Stage 4: Analytics
        logger.info("Stage 4: Running Analytics...")
        exec(open("/home/glue_user/stedi-project/scripts/query_data.py").read())
        logger.info("✓ Analytics complete")
        
        logger.info("\n🎉 Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    run_full_pipeline()
