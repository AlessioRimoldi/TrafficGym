import pkgutil
import importlib

from pathlib import Path

package_name = __name__
package_path = Path(__file__).parent

for _, module_name, is_pkg in pkgutil.iter_modules([str(package_path)]):
    if module_name.startswith("_"):
        continue

    importlib.import_module(f"{package_name}.{module_name}")
