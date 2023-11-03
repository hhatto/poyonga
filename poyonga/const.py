try:
    from enum import StrEnum
except ImportError:
    from enum import Enum
    class StrEnum(str, Enum):  # noqa
        pass


# protocol type
GRN_PROTO_GQTP = "gqtp"
GRN_PROTO_HTTP = "http"

# gqtp
GQTP_HEADER_SIZE = 24

# groonga status
GRN_STATUS_SUCCESS = 0
GRN_STATUS_END_OF_DATA = 1
GRN_STATUS_UNKNOWN_ERROR = 65535
GRN_STATUS_OPERATION_NOT_PERMITTED = 65534
GRN_STATUS_NO_SUCH_FILE_OR_DIRECTORY = 65533
GRN_STATUS_NO_SUCH_PROCESS = 65532
GRN_STATUS_INTERRUPTED_FUNCTION_CALL = 65531
GRN_STATUS_INPUT_OUTPUT_ERROR = 65530
GRN_STATUS_NO_SUCH_DEVICE_OR_ADDRESS = 65529
GRN_STATUS_ARG_LIST_TOO_LONG = 65528
GRN_STATUS_EXEC_FORMAT_ERROR = 65527
GRN_STATUS_BAD_FILE_DESCRIPTOR = 65526
GRN_STATUS_NO_CHILD_PROCESSES = 65525
GRN_STATUS_RESOURCE_TEMPORARILY_UNAVAILABLE = 65524
GRN_STATUS_NOT_ENOUGH_SPACE = 65523
GRN_STATUS_PERMISSION_DENIED = 65522
GRN_STATUS_BAD_ADDRESS = 65521
GRN_STATUS_RESOURCE_BUSY = 65520
GRN_STATUS_FILE_EXISTS = 65519
GRN_STATUS_IMPROPER_LINK = 65518
GRN_STATUS_NO_SUCH_DEVICE = 65517
GRN_STATUS_NOT_A_DIRECTORY = 65516
GRN_STATUS_IS_A_DIRECTORY = 65515
GRN_STATUS_INVALID_ARGUMENT = 65514
GRN_STATUS_TOO_MANY_OPEN_FILES_IN_SYSTEM = 65513
GRN_STATUS_TOO_MANY_OPEN_FILES = 65512
GRN_STATUS_INAPPROPRIATE_I_O_CONTROL_OPERATION = 65511
GRN_STATUS_FILE_TOO_LARGE = 65510
GRN_STATUS_NO_SPACE_LEFT_ON_DEVICE = 65509
GRN_STATUS_INVALID_SEEK = 65508
GRN_STATUS_READ_ONLY_FILE_SYSTEM = 65507
GRN_STATUS_TOO_MANY_LINKS = 65506
GRN_STATUS_BROKEN_PIPE = 65505
GRN_STATUS_DOMAIN_ERROR = 65504
GRN_STATUS_RESULT_TOO_LARGE = 65503
GRN_STATUS_RESOURCE_DEADLOCK_AVOIDED = 65502
GRN_STATUS_NO_MEMORY_AVAILABLE = 65501
GRN_STATUS_FILENAME_TOO_LONG = 65500
GRN_STATUS_NO_LOCKS_AVAILABLE = 65499
GRN_STATUS_FUNCTION_NOT_IMPLEMENTED = 65498
GRN_STATUS_DIRECTORY_NOT_EMPTY = 65497
GRN_STATUS_ILLEGAL_BYTE_SEQUENCE = 65496
GRN_STATUS_SOCKET_NOT_INITIALIZED = 65495
GRN_STATUS_OPERATION_WOULD_BLOCK = 65494
GRN_STATUS_ADDRESS_IS_NOT_AVAILABLE = 65493
GRN_STATUS_NETWORK_IS_DOWN = 65492
GRN_STATUS_NO_BUFFER = 65491
GRN_STATUS_SOCKET_IS_ALREADY_CONNECTED = 65490
GRN_STATUS_SOCKET_IS_NOT_CONNECTED = 65489
GRN_STATUS_SOCKET_IS_ALREADY_SHUTDOWNED = 65488
GRN_STATUS_OPERATION_TIMEOUT = 65487
GRN_STATUS_CONNECTION_REFUSED = 65486
GRN_STATUS_RANGE_ERROR = 65485
GRN_STATUS_TOKENIZER_ERROR = 65484
GRN_STATUS_FILE_CORRUPT = 65483
GRN_STATUS_INVALID_FORMAT = 65482
GRN_STATUS_OBJECT_CORRUPT = 65481
GRN_STATUS_TOO_MANY_SYMBOLIC_LINKS = 65480
GRN_STATUS_NOT_SOCKET = 65479
GRN_STATUS_OPERATION_NOT_SUPPORTED = 65478
GRN_STATUS_ADDRESS_IS_IN_USE = 65477
GRN_STATUS_ZLIB_ERROR = 65476
GRN_STATUS_LZO_ERROR = 65475
GRN_STATUS_STACK_OVER_FLOW = 65474
GRN_STATUS_SYNTAX_ERROR = 65473
GRN_STATUS_RETRY_MAX = 65472
GRN_STATUS_INCOMPATIBLE_FILE_FORMAT = 65471
GRN_STATUS_UPDATE_NOT_ALLOWED = 65470
GRN_STATUS_TOO_SMALL_OFFSET = 65469
GRN_STATUS_TOO_LARGE_OFFSET = 65468
GRN_STATUS_TOO_SMALL_LIMIT = 65467
GRN_STATUS_CAS_ERROR = 65466
GRN_STATUS_UNSUPPORTED_COMMAND_VERSION = 65465


class InputType(StrEnum):
    JSON = "json"
    APACHE_ARROW = "apache-arrow"


class OutputType(StrEnum):
    JSON = "json"
    TSV = "tsv"
    MSGPACK = "msgpack"
    APACHE_ARROW = "apache-arrow"
