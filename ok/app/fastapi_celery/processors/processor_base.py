# Standard Library Imports
from datetime import datetime
import logging
from models.tracking_models import TrackingModel
from processors.processor_nodes import PROCESS_FUNCTIONS
from utils import log_helpers
import importlib
import inspect
import types
from models.class_models import StepDefinition
from utils.middlewares.request_context import get_context_value

# ===
# Set up logging
logger_name = "ProcessorBase"
log_helpers.logging_config(logger_name)
base_logger = logging.getLogger(logger_name)

# Wrap the base logger with the adapter
logger = log_helpers.ValidatingLoggerAdapter(base_logger, {})
# ===


class ProcessorBase:
    def __init__(self, tracking_model: TrackingModel):
        self.file_record = {}
        self.source_type = None
        self.document_type = None
        self.target_bucket_name = None
        self.current_time = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
        self.tracking_model = tracking_model
        self._register_processor_functions()
        self.extract_metadata()

    def _register_processor_functions(self) -> None:

        base_module = "processors.process_functions"

        for process in PROCESS_FUNCTIONS:
            try:
                module = importlib.import_module(f"{base_module}.{process}")
                for name, func in inspect.getmembers(module, inspect.isfunction):
                    bound_method = types.MethodType(func, self)
                    setattr(self, name, bound_method)
                    logger.debug(f"Registered process function: {name} from {process}")
            except ModuleNotFoundError:
                logger.warning(f"Module {process} not found in process_nodes.")
