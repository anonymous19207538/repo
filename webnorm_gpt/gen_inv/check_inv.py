import datetime
import traceback
import typing
from abc import ABC, ABCMeta, abstractmethod

from ..file_types.log_file import LogFile, LogItem
from .base import APIDomainAllPlaceholder, Field, Invariant, Predicate, RelatedFields


def run_py_predicate(
    predicate: Predicate,
    log_items: list[LogItem],
    fields: list[list[Field]],
) -> tuple[bool, typing.Optional[str]]:

    if predicate.is_true_predicate:
        return True

    if len(fields) != len(log_items):
        raise ValueError("Length of fields and log_items should be the same.")

    if len(fields) != predicate.num_args:
        raise ValueError(
            f"Number of arguments in predicate should be {len(fields)}, but got {predicate.num_args}"
        )

    input_args = []

    for log_item, field in zip(log_items, fields):
        input_args.append(log_item.to_check_dict(field))

    if predicate.num_args != len(fields):
        raise ValueError(
            f"Number of arguments in predicate should be {len(fields)}, but got {predicate.num_args}"
        )

    try:
        ret = predicate.py_func(*input_args)
    except Exception as e:
        return False, "".join(traceback.format_exception_only(e))

    if ret is False:
        return False, ValueError("check function return False")

    return True, None


def format_exc_in_string(code: str, e: Exception) -> str:
    to_append = []

    code_lines = code.split("\n")

    tb = e.__traceback__
    while tb is not None:
        code_file_name = tb.tb_frame.f_code.co_filename
        if code_file_name == "<string>":
            lineno = tb.tb_lineno - 1
            method_name = tb.tb_frame.f_code.co_name
            to_append.append(f"At line {tb.tb_lineno} in {method_name}")

            lines_before = code_lines[max(0, lineno - 2) : lineno]
            lines_after = code_lines[lineno + 1 : min(len(code_lines), lineno + 3)]
            line = code_lines[lineno]

            for l in lines_before:
                to_append.append(f"  {l}")
            to_append.append(f">>{line}")

            for l in lines_after:
                to_append.append(f"  {l}")

            to_append.append("")

        tb = tb.tb_next
    to_append.append("".join(traceback.format_exception_only(e)))

    return "\n".join(to_append)


def run_py_predicate_new_json_format(
    predicate: Predicate,
    log_items: list[LogItem],
    related_fields: list[RelatedFields],
) -> tuple[bool, typing.Optional[str]]:

    if predicate.is_true_predicate:
        return True, None

    input_args = []

    for log_item, fields in zip(log_items, related_fields):
        input_args.append(log_item.to_execute_json(fields))

    if predicate.num_args != len(related_fields):
        raise ValueError(
            f"Number of arguments in predicate should be {len(related_fields)}, but got {predicate.num_args}"
        )

    try:
        ret = predicate.py_func(*input_args)
    except Exception as e:
        error_info = format_exc_in_string(predicate.py_code, e)
        # print("CAUGHT ERROR: ", error_info)
        return False, error_info

    if ret is False:
        return False, "check function return False"

    return True, None


def is_two_events_related(log1: LogItem, log2: LogItem) -> bool:
    format_str = "%Y-%m-%d %H:%M:%S.%f"
    t1 = datetime.datetime.strptime(log1.time, format_str)
    t2 = datetime.datetime.strptime(log2.time, format_str)
    if abs((t1 - t2).total_seconds()) < 600:
        return True


def find_nearest_related_event(
    log: LogItem,
    log_idx: int,
    all_log: LogFile,
    other_api: typing.Union[str, APIDomainAllPlaceholder],
    direction: typing.Literal["before", "after"],
) -> tuple[int, typing.Optional[LogItem]]:
    if direction == "before":
        find_range = range(log_idx - 1, -1, -1)
    elif direction == "after":
        find_range = range(log_idx + 1, len(all_log))
    else:
        raise ValueError(f"Invalid direction: {direction}")

    for i in find_range:
        other_log = all_log[i]
        if isinstance(other_api, APIDomainAllPlaceholder) or other_log.api == other_api:
            if is_two_events_related(log, other_log):
                return i, other_log

    return -1, None


class InvChecker(ABC):
    @abstractmethod
    def can_check(self, inv: Invariant) -> bool:
        pass

    @abstractmethod
    def is_related(self, inv: Invariant, log: LogItem) -> bool:
        pass

    def get_all_related_event_api(self, inv: Invariant) -> typing.Optional[list[str]]:
        return None

    @abstractmethod
    def check(
        self, inv: Invariant, log: LogItem, log_idx: int, all_log: LogFile
    ) -> tuple[bool, typing.Optional[str]]:
        pass


class InvCheckerForAllOne(InvChecker):
    def can_check(self, inv):
        return (
            len(inv.domain) == 1
            and inv.domain[0].quantifier == "forall"
            and inv.premise.premise_type == "true"
        )

    def is_related(self, inv, log):
        return inv.domain[0].api == log.api

    def get_all_related_event_api(self, inv):
        return [inv.domain[0].api]

    def check(self, inv, log, log_idx, all_log) -> tuple[bool, typing.Optional[str]]:
        return run_py_predicate(inv.predicate, [log], [inv.domain[0].related_fields])


class InvCheckerForAllExistsNearestReleatd(InvChecker):
    def can_check(self, inv):
        print(len(inv.domain))
        print(inv.domain[0].quantifier)
        print(inv.domain[1].quantifier)
        print(inv.premise.premise_position)
        print(inv.premise.premise_type)
        return (
            len(inv.domain) == 2
            and inv.domain[0].quantifier == "forall"
            and inv.domain[1].quantifier == "exists"
            and inv.premise.premise_position == "and"
            and inv.premise.premise_type
            in ["two_second_nearest_after_first", "two_first_nearest_after_second"]
        )

    def is_related(self, inv, log):
        return inv.domain[0].api == log.api

    def get_all_related_event_api(self, inv):
        return [inv.domain[0].api]

    def check(self, inv, log, log_idx, all_log) -> tuple[bool, typing.Optional[str]]:
        if inv.premise.premise_type == "two_first_nearest_after_second":
            direction = "before"
        else:
            direction = "after"

        second_log_idx, second_log = find_nearest_related_event(
            log, log_idx, all_log, inv.domain[1].api, direction
        )

        if second_log_idx == -1:
            return False, "Cannot find related event"

        if inv.premise.premise_type == "two_first_nearest_after_second":
            fields = [[inv.domain[1]], [inv.domain[0]]]
            log_items = [second_log, log]
        else:
            fields = [[inv.domain[0]], [inv.domain[1]]]
            log_items = [log, second_log]

        fields = [x.related_fields for x in inv.domain]

        return run_py_predicate(inv.predicate, log_items, fields)


def get_all_inv_checkers() -> list[InvChecker]:
    results = []
    for key, item in globals().items():
        if type(item) != ABCMeta:
            continue
        if issubclass(item, InvChecker) and item != InvChecker:
            results.append(item())
    return results


ALL_INV_CHECKERS = get_all_inv_checkers()


def find_checker(inv: Invariant) -> typing.Optional[InvChecker]:
    for checker in ALL_INV_CHECKERS:
        if checker.can_check(inv):
            return checker
    return None
