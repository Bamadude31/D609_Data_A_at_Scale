# Use AWS Glue base image
FROM public.ecr.aws/glue/aws-glue-libs:glue_libs_4.0.0_image_01

# Set working directory
WORKDIR /home/glue_user/stedi-project

# Copy ONLY landing zone data and scripts (not trusted/curated folders)
COPY data/landing/ ./data/landing/
COPY scripts/ ./scripts/
