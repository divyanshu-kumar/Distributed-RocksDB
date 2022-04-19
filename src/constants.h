enum DebugLevel { LevelInfo = 0, LevelError = 1, LevelNone = 2 };
const DebugLevel debugMode = LevelInfo;


const int one_kb = 1024;
const int one_mb = 1024 * one_kb;
const int one_gb = 1024 * one_mb;
const int MAX_SIZE_BYTES = one_gb / 10;
const int BLOCK_SIZE_BYTES = 4 * one_kb;
const int numBlocks = MAX_SIZE_BYTES / BLOCK_SIZE_BYTES;
const int stalenessLimit = 10 * 1e3; // milli-seconds
const int SERVER_OFFLINE_ERROR_CODE = -1011317;

const int backReadStalenessLimit = 10;