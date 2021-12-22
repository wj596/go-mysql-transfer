/*
 * Copyright 2021-2022 the original author(https://github.com/wj596)
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

package filter

import (
	"github.com/gin-gonic/gin"

	"go-mysql-transfer/config"
	"go-mysql-transfer/domain/constants"
	"go-mysql-transfer/util/httputils"
	"go-mysql-transfer/util/stringutils"
)

// SignFilter 签名验证中间件
func SignFilter() gin.HandlerFunc {
	return func(c *gin.Context) {
		timestamp := c.Request.Header.Get(constants.HeaderParamTimestamp)
		sign := c.Request.Header.Get(constants.HeaderParamSign)
		std := httputils.Sign(stringutils.ToInt64Safe(timestamp), config.GetIns().SecretKey)
		if sign != std {
			c.AbortWithStatus(401)
			return
		}

		c.Next()
	}
}
