"""测试收集前关闭文件日志，避免读取或清理真实用户日志目录。"""

import os


os.environ["BIGQMT_LOG_ENABLED"] = "0"
