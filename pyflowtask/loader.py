#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import abc
import importlib
import inspect
from collections import defaultdict

from pyflowtask import BaseTask


class TaskLoader(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        self.task_flow_store = defaultdict(list)

    @abc.abstractmethod
    def load(self, *a, **ka):
        pass


class ModuleTaskLoader(FlowLoader):
    """Python Module File based task loader
    """

    def load(self, module_name):
        """load python module file

        Parameters:
        - module_name: (str) python module full name 

        Returns: 
            {
                flow_name: 
                    [
                        (task_cls_1, task_priority_1), 
                        (task_cls_2, task_priority_2), 
                        (...), ...
                    ]
            }
        """
        
        task_module = importlib.import_module(module_name)
        for k, v in task_module.__dict__.iteritems():
            # ignore '__**' keys
            if k.startswith('__'):
                continue
            # ignore the ones not a class
            if not inspect.isclass(v):
                continue
            # ignore not BaseTask ones
            if not isinstance(v, BaseTask):
                continue
            #   支持同一个taskflow设置不同的task-weight
            #   flow_attrs=((flowname, priority), ...)
            if hasattr(v, 'flow_attrs'):
                flow_attrs = getattr(v, 'flow_attrs')
                if isinstance(flow_attrs, list):
                    for flow_name, priority in flow_attrs:
                        self.task_flow_store[flow_name].append(
                            (v, priority))
            else:
                # get priority from Task object
                if hasattr(v, 'priority'):
                    priority = getattr(v, 'priority')
                else:
                    priority = BaseTask.DEFAULT_PRIORITY
                flow_name = getattr(v, 'flow_name')
                if isinstance(flow_name, list):
                    for a_flow_name in flow_name:
                        self.task_flow_store[a_flow_name].append(
                            (v, priority))
                else:
                    self.task_flow_store[flow_name].append(
                        (v, priority))
        return self.task_flow_store


class DirTaskLoader(FlowLoader):
    """Directory File based task loader
    """

    def load(self, dir_path, module_prefix=''):
        # if dir not exists, return NOW
        if not os.path.exists(dir_path):
            return self.task_flow_store 

        # List all files in directory
        for task_file_path in os.listdir(dir_path):
            module_loader.clean()
            # Only .py file will be scanned, except __init__.py
            if not task_file_path.endswith('.py') \
                    or task_file_path.startswith('__'):
                continue

            # file_module = module_prefix + . + abc(-.py)
            file_module_name = ''.join([module_prefix, '.', task_file_path[:-3]])
            tmp_task_flows = ModuleTaskLoader().load(file_module_name)
            for flow_name, task_priority_tuple_list in tmp_task_flows.iteritems():
                if flow_name in self.task_flow_store:
                    self.task_flow_store[flow_name].extend(task_priority_tuple_list)
                        
        return self.task_flow_store

