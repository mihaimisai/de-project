from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, List, Optional, Set, Tuple, TypedDict

from botocore.client import BaseClient
from botocore.exceptions import ClientError

from moto.stepfunctions.parser.api import (
    HistoryEventType,
    LoggingConfiguration,
    LogLevel,
    LongArn,
)
from moto.utilities.arns import (
    parse_arn,
)

LOG = logging.getLogger(__name__)

ExecutionEventLogDetails = dict

# The following event type sets are compiled according to AWS's
# log level definitions: https://docs.aws.amazon.com/step-functions/latest/dg/cloudwatch-log-level.html
_ERROR_LOG_EVENT_TYPES: Set[HistoryEventType] = {
    HistoryEventType.ExecutionAborted,
    HistoryEventType.ExecutionFailed,
    HistoryEventType.ExecutionTimedOut,
    HistoryEventType.FailStateEntered,
    HistoryEventType.LambdaFunctionFailed,
    HistoryEventType.LambdaFunctionScheduleFailed,
    HistoryEventType.LambdaFunctionStartFailed,
    HistoryEventType.LambdaFunctionTimedOut,
    HistoryEventType.MapStateAborted,
    HistoryEventType.MapStateFailed,
    HistoryEventType.MapIterationAborted,
    HistoryEventType.MapIterationFailed,
    HistoryEventType.MapRunAborted,
    HistoryEventType.MapRunFailed,
    HistoryEventType.ParallelStateAborted,
    HistoryEventType.ParallelStateFailed,
    HistoryEventType.TaskFailed,
    HistoryEventType.TaskStartFailed,
    HistoryEventType.TaskStateAborted,
    HistoryEventType.TaskSubmitFailed,
    HistoryEventType.TaskTimedOut,
    HistoryEventType.WaitStateAborted,
}
_FATAL_LOG_EVENT_TYPES: Set[HistoryEventType] = {
    HistoryEventType.ExecutionAborted,
    HistoryEventType.ExecutionFailed,
    HistoryEventType.ExecutionTimedOut,
}


# The LogStreamName used when creating the empty Log Stream when validating the logging configuration.
VALIDATION_LOG_STREAM_NAME: str = (
    "log_stream_created_by_aws_to_validate_log_delivery_subscriptions"
)


def is_logging_enabled_for(
    log_level: LogLevel, history_event_type: HistoryEventType
) -> bool:
    # Checks whether the history event type is in the context of a give LogLevel.
    if log_level == LogLevel.ALL:
        return True
    elif log_level == LogLevel.OFF:
        return False
    elif log_level == LogLevel.ERROR:
        return history_event_type in _ERROR_LOG_EVENT_TYPES
    elif log_level == LogLevel.FATAL:
        return history_event_type in _FATAL_LOG_EVENT_TYPES
    else:
        LOG.error("Unknown LogLevel '%s'", log_level)


class CloudWatchLoggingConfiguration:
    state_machine_arn: LongArn
    log_level: LogLevel
    log_account_id: str
    log_region: str
    log_group_name: str
    log_stream_name: str
    include_execution_data: bool

    def __init__(
        self,
        state_machine_arn: LongArn,
        log_account_id: str,
        log_region: str,
        log_group_name: str,
        log_level: LogLevel,
        include_execution_data: bool,
    ):
        self.state_machine_arn = state_machine_arn
        self.log_level = log_level
        self.log_group_name = log_group_name
        self.log_account_id = log_account_id
        self.log_region = log_region
        # TODO: AWS appears to append a date and a serial number to the log
        #  stream name: more investigations are needed in this area.
        self.log_stream_name = f"states/{state_machine_arn}"
        self.include_execution_data = include_execution_data

    @staticmethod
    def extract_log_arn_parts_from(
        logging_configuration: LoggingConfiguration,
    ) -> Optional[Tuple[str, str, str]]:
        # Returns a tuple with: account_id, region, and log group name if the logging configuration
        # specifies a valid cloud watch log group arn, none otherwise.

        destinations = logging_configuration.get("destinations")
        if (
            not destinations or len(destinations) > 1
        ):  # Only one destination can be defined.
            return None

        log_group = destinations[0].get("cloudWatchLogsLogGroup")
        if not log_group:
            return None

        log_group_arn = log_group.get("logGroupArn")
        if not log_group_arn:
            return None

        try:
            arn_data = parse_arn(log_group_arn)
        except IndexError:
            return None

        log_region = arn_data.region
        if log_region is None:
            return None

        log_account_id = arn_data.account
        if log_account_id is None:
            return None

        log_resource = arn_data.resource_id
        if log_resource is None:
            return None

        log_resource_parts = log_resource.split("log-group:")
        if not log_resource_parts:
            return None

        log_group_name = log_resource_parts[-1].split(":")[0]
        return log_account_id, log_region, log_group_name

    @staticmethod
    def from_logging_configuration(
        state_machine_arn: LongArn,
        logging_configuration: LoggingConfiguration,
    ) -> Optional[CloudWatchLoggingConfiguration]:
        log_level = logging_configuration.get("level", LogLevel.OFF)
        if log_level == LogLevel.OFF:
            return None

        log_arn_parts = CloudWatchLoggingConfiguration.extract_log_arn_parts_from(
            logging_configuration=logging_configuration
        )
        if not log_arn_parts:
            return None
        log_account_id, log_region, log_group_name = log_arn_parts

        include_execution_data = logging_configuration["includeExecutionData"]

        return CloudWatchLoggingConfiguration(
            state_machine_arn=state_machine_arn,
            log_account_id=log_account_id,
            log_region=log_region,
            log_group_name=log_group_name,
            log_level=log_level,
            include_execution_data=include_execution_data,
        )

    def validate(self) -> None:
        pass


class HistoryLog(TypedDict):
    id: str
    previous_event_id: str
    event_timestamp: datetime
    type: HistoryEventType
    execution_arn: LongArn
    details: Optional[ExecutionEventLogDetails]


class CloudWatchLoggingSession:
    execution_arn: LongArn
    configuration: CloudWatchLoggingConfiguration
    _logs_client: BaseClient
    _is_log_stream_available: bool

    def __init__(
        self, execution_arn: LongArn, configuration: CloudWatchLoggingConfiguration
    ):
        self.execution_arn = execution_arn
        self.configuration = configuration
        self._logs_client = None

    def log_level_filter(self, history_event_type: HistoryEventType) -> bool:
        # Checks whether the history event type should be logged in this session.
        return is_logging_enabled_for(
            log_level=self.configuration.log_level,
            history_event_type=history_event_type,
        )

    def publish_history_log(self, history_log: HistoryLog) -> None:
        pass

    def _publish_history_log_or_setup(self, log_events: List[Any]):
        # Attempts to put the events into the given log group and stream, and attempts to create the stream if
        # this does not already exist.
        is_events_put = self._put_events(log_events=log_events)
        if is_events_put:
            return

        is_setup = self._setup()
        if not is_setup:
            LOG.debug(
                "CloudWatch Log was not published due to setup errors encountered "
                "while creating the LogStream for execution '%s'.",
                self.execution_arn,
            )
            return

        self._put_events(log_events=log_events)

    def _put_events(self, log_events: List[Any]) -> bool:
        # Puts the events to the targe log group and stream, and returns false if the LogGroup or LogStream could
        # not be found, true otherwise.
        try:
            self._logs_client.put_log_events(
                logGroupName=self.configuration.log_group_name,
                logStreamName=self.configuration.log_stream_name,
                logEvents=log_events,
            )
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code == "ResourceNotFoundException":
                return False
        except Exception as ignored:
            LOG.warning(
                "State Machine execution log event could not be published due to an error: '%s'",
                ignored,
            )
        return True

    def _setup(self) -> bool:
        # Create the log stream if one does not exist already.
        # TODO: enhance the verification logic to match AWS's logic to ensure IAM features work as expected.
        #  https://docs.aws.amazon.com/step-functions/latest/dg/cw-logs.html#cloudwatch-iam-policy
        try:
            self._logs_client.create_log_stream(
                logGroupName=self.configuration.log_group_name,
                logStreamName=self.configuration.log_stream_name,
            )
        except ClientError as error:
            error_code = error.response["Error"]["Code"]
            if error_code != "ResourceAlreadyExistsException":
                LOG.error(
                    "Could not create execution log stream for execution '%s' due to %s",
                    self.execution_arn,
                    error,
                )
                return False
        return True
