"""
Utility module to inspect external python projects.
"""
import imp
import inspect
import os
import sys

__author__ = "Joan Grau, @grautxo"


def is_list(obj):
    """
    Check if @obj is instance of list or tuple.
    """
    return isinstance(obj, (list, tuple,))


def to_list(obj, class_type=list):
    """
    Returns the @obj if is a list. Returns a tuple with the @obj otherwise.
    """
    return class_type(obj if is_list(obj) else (obj,))


def to_tuple(obj):
    """
    Returns the @obj if is a tuple. Returns a tuple with the @obj otherwise.
    """
    return to_list(obj, class_type=tuple)


def import_module(file_path='', sep=os.path.sep, pyclean=True):
    """
    Imports a module from a gitven @file_path.
    """
    if sep not in file_path:
        file_path = file_path.replace('.', sep)

    path, ext = os.path.splitext(file_path)
    package = os.path.basename(path).replace(sep, '.')
    dirname = os.path.dirname(file_path)

    if dirname not in sys.path:
        sys.path.insert(0, dirname)

    try:
        module = import_non_local(package, paths=dirname)
        return module
    finally:
        sys.path.remove(dirname)
        if pyclean:
            pyc_path = os.path.join(dirname, '%s.pyc' % package)
            if os.path.exists(pyc_path):
                os.remove(pyc_path)


def import_non_local(name, custom_name=None, paths=None):
    """
    Returns the module named as @name. If @custom_name is provided the module
    will be named as @custom_name internally.
    """
    paths = [path for path in to_tuple(paths or '') if path]
    custom_name = custom_name or name

    f, pathname, desc = imp.find_module(name, paths or sys.path[1:])
    module = imp.load_module(custom_name, f, pathname, desc)
    if f:
        f.close()

    return module


def search_subclasses(module_path, parent_classes):
    """
    Returns a list of classes if the class is a subclass from any of
    @parent_classes on the specified @module_path.
    Argument @parent_classes can be a class, a list or a tuple of classes.
    """
    class_set = set()
    parent_classes = to_tuple(parent_classes)
    files = os.listdir(module_path)

    # Ensure that is a python module
    if '__init__.py' not in files:
        return []

    # Get all subclasses on each submodule
    for f in files:
        if f == '__init__.py' or not f.endswith('.py'):
            continue
        file_name, ext = os.path.splitext(f)
        package = os.path.join(module_path, file_name).replace(os.sep, '.')
        # Import the module and get the classes
        module = import_module(package.replace('.py', ''))
        for value in module.__dict__.values():
            # Add the class to the results if value is a subclass of
            # any of @parent_classes
            if (inspect.isclass(value) and
               not [p for p in parent_classes if value is p] and
               issubclass(value, parent_classes)):
                class_set.add(value)

    return list(class_set)
