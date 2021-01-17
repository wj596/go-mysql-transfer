/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package dates

import (
	"strings"
)

const (
	yyyy = "2006"
	yy   = "06"
	mmmm = "January"
	mmm  = "Jan"
	mm   = "04"
	m    = "1"

	dddd = "Monday"
	ddd  = "Mon"
	dd   = "02"

	HHT = "03"
	HH  = "15"
	MM  = "01"
	SS  = "05"
	ss  = "05"
	tt  = "PM"
	Z   = "MST"
	ZZZ = "MST"

	o = "Z07:00"
)

func ConvertGoFormat(format string) string {
	var goFormate = format
	if strings.Contains(goFormate, "YYYY") {
		goFormate = strings.Replace(goFormate, "YYYY", yyyy, -1)
	} else if strings.Contains(goFormate, "yyyy") {
		goFormate = strings.Replace(goFormate, "yyyy", yyyy, -1)
	} else if strings.Contains(goFormate, "YY") {
		goFormate = strings.Replace(goFormate, "YY", yy, -1)
	} else if strings.Contains(goFormate, "yy") {
		goFormate = strings.Replace(goFormate, "yy", yy, -1)
	}

	if strings.Contains(goFormate, "MMMM") {
		goFormate = strings.Replace(goFormate, "MMMM", mmmm, -1)
	} else if strings.Contains(goFormate, "mmmm") {
		goFormate = strings.Replace(goFormate, "mmmm", mmmm, -1)
	} else if strings.Contains(goFormate, "MMM") {
		goFormate = strings.Replace(goFormate, "MMM", mmm, -1)
	} else if strings.Contains(goFormate, "mmm") {
		goFormate = strings.Replace(goFormate, "mmm", mmm, -1)
	} else if strings.Contains(goFormate, "mm") {
		goFormate = strings.Replace(goFormate, "mm", mm, -1)
	}

	if strings.Contains(goFormate, "dddd") {
		goFormate = strings.Replace(goFormate, "dddd", dddd, -1)
	} else if strings.Contains(goFormate, "ddd") {
		goFormate = strings.Replace(goFormate, "ddd", ddd, -1)
	} else if strings.Contains(goFormate, "dd") {
		goFormate = strings.Replace(goFormate, "dd", dd, -1)
	}

	if strings.Contains(goFormate, "tt") {
		if strings.Contains(goFormate, "HH") {
			goFormate = strings.Replace(goFormate, "HH", HHT, -1)
		} else if strings.Contains(goFormate, "hh") {
			goFormate = strings.Replace(goFormate, "hh", HHT, -1)
		}
		goFormate = strings.Replace(goFormate, "tt", tt, -1)
	} else {
		if strings.Contains(goFormate, "HH") {
			goFormate = strings.Replace(goFormate, "HH", HH, -1)
		} else if strings.Contains(goFormate, "hh") {
			goFormate = strings.Replace(goFormate, "hh", HH, -1)
		}
		goFormate = strings.Replace(goFormate, "tt", "", -1)
	}

	if strings.Contains(goFormate, "MM") {
		goFormate = strings.Replace(goFormate, "MM", MM, -1)
	}

	if strings.Contains(goFormate, "SS") {
		goFormate = strings.Replace(goFormate, "SS", SS, -1)
	} else if strings.Contains(goFormate, "ss") {
		goFormate = strings.Replace(goFormate, "ss", SS, -1)
	}

	if strings.Contains(goFormate, "ZZZ") {
		goFormate = strings.Replace(goFormate, "ZZZ", ZZZ, -1)
	} else if strings.Contains(goFormate, "zzz") {
		goFormate = strings.Replace(goFormate, "zzz", ZZZ, -1)
	} else if strings.Contains(goFormate, "Z") {
		goFormate = strings.Replace(goFormate, "Z", Z, -1)
	} else if strings.Contains(goFormate, "z") {
		goFormate = strings.Replace(goFormate, "z", Z, -1)
	}

	if strings.Contains(goFormate, "tt") {
		goFormate = strings.Replace(goFormate, "tt", tt, -1)
	}
	if strings.Contains(goFormate, "o") {
		goFormate = strings.Replace(goFormate, "o", o, -1)
	}

	return goFormate
}
