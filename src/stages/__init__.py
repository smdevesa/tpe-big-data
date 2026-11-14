from src.stages import bronze_batch, bronze_stream, gold, serving, silver

STAGES = [
    ("bronze_batch", bronze_batch.run),
    ("bronze_stream", bronze_stream.run),
    ("silver", silver.run),
    ("gold", gold.run),
    ("serving", serving.run),
]

STAGE_NAMES = [name for name, _ in STAGES]
