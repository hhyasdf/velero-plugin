/*
Copyright 2018, 2019 the Velero contributors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package alibabacloud

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestImageTransform(t *testing.T) {
	test := []map[string]interface{}{
		{
			"test": map[string]interface{}{
				"image": "test",
				"test": map[string]interface{}{
					"test": "test",
				},
			},
		},
		{
			"test": map[string]interface{}{
				"test":  "",
				"test1": "",
				"test2": map[string]interface{}{
					"image": "test",
				},
			},
			"test1": "test1",
		},
		{
			"image": "test",
		},
		{},
	}

	for _, item := range test {
		OperateObjectFields(item, "image", map[string]string{"tes": "1"}, ImageTransform)
	}

	assert.Equal(t, "1t", test[0]["test"].(map[string]interface{})["image"].(string))
	assert.Equal(t, "1t", test[1]["test"].(map[string]interface{})["test2"].(map[string]interface{})["image"].(string))
	assert.Equal(t, "1t", test[2]["image"].(string))
	assert.Empty(t, test[3])
}
