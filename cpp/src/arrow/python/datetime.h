// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef PYARROW_UTIL_DATETIME_H
#define PYARROW_UTIL_DATETIME_H

#include <algorithm>
#include <chrono>

#include "arrow/python/platform.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace py {
namespace internal {

int64_t PyTime_to_us(PyObject* pytime);

inline int64_t PyTime_to_s(PyObject* pytime) { return PyTime_to_us(pytime) / 1000000; }

inline int64_t PyTime_to_ms(PyObject* pytime) { return PyTime_to_us(pytime) / 1000; }

inline int64_t PyTime_to_ns(PyObject* pytime) { return PyTime_to_us(pytime) * 1000; }

Status PyTime_from_int(int64_t val, const TimeUnit::type unit, PyObject** out);
Status PyDate_from_int(int64_t val, const DateUnit unit, PyObject** out);
Status PyDateTime_from_int(int64_t val, const TimeUnit::type unit, PyObject** out);

using TimePoint =
    std::chrono::time_point<std::chrono::system_clock, std::chrono::nanoseconds>;

Status PyDateTime_from_TimePoint(TimePoint val, PyObject** out);

int64_t PyDate_to_days(PyDateTime_Date* pydate);

inline int64_t PyDate_to_ms(PyDateTime_Date* pydate) {
  return PyDate_to_days(pydate) * 24 * 3600 * 1000;
}

int64_t PyDateTime_to_s(PyDateTime_DateTime* pydatetime);

inline int64_t PyDateTime_to_ms(PyDateTime_DateTime* pydatetime) {
  int64_t date_ms = PyDateTime_to_s(pydatetime) * 1000;
  int ms = PyDateTime_DATE_GET_MICROSECOND(pydatetime) / 1000;
  return date_ms + ms;
}

inline int64_t PyDateTime_to_us(PyDateTime_DateTime* pydatetime) {
  int64_t ms = PyDateTime_to_s(pydatetime) * 1000;
  int us = PyDateTime_DATE_GET_MICROSECOND(pydatetime);
  return ms * 1000 + us;
}

inline int64_t PyDateTime_to_ns(PyDateTime_DateTime* pydatetime) {
  return PyDateTime_to_us(pydatetime) * 1000;
}

}  // namespace internal
}  // namespace py
}  // namespace arrow

#endif  // PYARROW_UTIL_DATETIME_H
