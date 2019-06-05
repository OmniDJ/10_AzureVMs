import os


def load_module(module_name, file_name):
  """
  loads modules from _pyutils Google Drive repository
  usage:
    module = load_module("logger", "logger.py")
    logger = module.Logger()
  """
  from importlib.machinery import SourceFileLoader
  home_dir = os.path.expanduser("~")
  valid_paths = [
                   os.path.join(home_dir, "Lummetry.AIDropbox/SRC"),
                   os.path.join(home_dir, "Lummetry.AI Dropbox/SRC"),
                   os.path.join(os.path.join(home_dir, "Desktop"), "Lummetry.AI Dropbox/SRC"),
                   os.path.join(os.path.join(home_dir, "Desktop"), "Lummetry.AIDropbox/SRC"),
                   os.path.join("C:/", "Lummetry.AI Dropbox/SRC"),
                   os.path.join("C:/", "Lummetry.AIDropbox/SRC"),
                   os.path.join("D:/", "Lummetry.AI Dropbox/SRC"),
                   os.path.join("D:/", "Lummetry.AIDropbox/SRC"),
                   ]

  drive_path = None
  for path in valid_paths:
    if os.path.isdir(path):
      drive_path = path
      break

  if drive_path is None:
    logger_lib = None
    print("Logger library not found in shared repo.", flush = True)
    #raise Exception("Couldn't find google drive folder!")
  else:  
    utils_path = os.path.join(drive_path, "_pyutils")
    print("Loading [{}] package...".format(os.path.join(utils_path,file_name)),flush = True)
    logger_lib = SourceFileLoader(module_name, os.path.join(utils_path, file_name)).load_module()
    print("Done loading [{}] package.".format(os.path.join(utils_path,file_name)),flush = True)

  return logger_lib

class SimpleLogger: 
  def __init__(self):
    return
  def VerboseLog(self, _str, show_time):
    print(_str, flush = True)


def LoadLogger(lib_name, config_file, load_tf=True, show_prompt=True):
  module = load_module("logger", "logger.py")
  if module is not None:
    logger = module.Logger(lib_name=lib_name, config_file=config_file,
                           TF_KERAS=load_tf, SHOW_TIME=show_prompt)
  else:
    logger = SimpleLogger()
  return logger