#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, with_statement

import sys

from taskflow import task, engines
from taskflow.patterns import linear_flow as lflow
from taskflow.listeners import printing, timing


# store the all the flow name and task / priority list
#    {'flowname': [(Task_Cls, priority), (Task_Cls, priority), ...]}
app_task_flows = defaultdict(list)


class BaseTask(task.Task):
    """Base Task class, extends task.Task. 
    It support flow name and flow priority value.
    """

    # indicates the priority of this task in the taskflow
    default_priority = 500

    # indicates the Task properties
    #   name (str): the name of task
    #   provides (tuple(str) or str): the returned names of vars
    #   requires (tuple(str) or str): the names of params of execute method
    properties = {
        'name': None,
        'provides': None,
        'requires': None,
    }

    # indicates the flow names which this task belongs to.
    # it could be a str or a str list,
    #       str:              the name of the flow
    #       list(str):        the names of all the flows with the same priority
    flow_name = None

    # indicates the complex (flowname, priority) tuple list.
    # it may overrides `flow_name` and `default_priority` vars when not None
    flow_attrs = None

    def execute(self, *a, **ka):
        pass

    def revert(self, result, *a, **ka):
        pass


def run_flow(flow_name, init_params=None):
    """ run the tasks in given flow name
    """
    """ actual taskflow runner
    """
    if flow_name not in app_task_flows:
        raise Exception('taskflow-%s not definied' % flow_name)

    flow = lflow.Flow(flow_name)

    for task_cls, _ in app_task_flows[flow_name]:
        task_params = getattr(task_cls, 'properties')
        if isinstance(task_params, dict):
            flow.add(task_cls(**task_params))

    eng = engines.load(flow, store=init_params or {})

    if sys.version_info > (2, 7):
        with printing.PrintingListener(eng), timing.PrintingDurationListener(eng):
            eng.run()
    else:
        with nested(printing.PrintingListener(eng), timing.PrintingDurationListener(eng)):
            eng.run()

    return eng.storage.fetch_all()
