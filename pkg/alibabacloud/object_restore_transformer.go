/*
Copyright 2017, 2019 the Velero contributors.
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/heptio/velero/pkg/plugin/velero"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

type TransformConfig struct {
	// OLD_IMAGE_SOURCE : NEW_IMAGE_SOURCE
	ImageSource map[string]string `json:"ImageSources"`

	// PERSISTENT_VOLUME_NAME : info struct
	PersistentVolume map[string](map[string]string) `json:"PersistentVolume"`

	// STORAGE_CLASS_NAME : info structs
	StorageClass map[string](map[string]string) `json:"StorageClass"`
}

type ObjectRestoreTransformer struct {
	conf TransformConfig
	log  logrus.FieldLogger
}

// NewObjectRestoreTransformer init a ObjectRestoreTransformer
func NewObjectRestoreTransformer(logger logrus.FieldLogger) *ObjectRestoreTransformer {
	new := &ObjectRestoreTransformer{log: logger}
	if err := new.LoadConfig(); err != nil {
		new.log.Errorf("Config load err: %v", err)
	}
	return new
}

// this action will apply to all of the objects which need to be restored
func (p *ObjectRestoreTransformer) AppliesTo() (velero.ResourceSelector, error) {
	// try to reload configuration
	p.LoadConfig()
	return velero.ResourceSelector{}, nil
}

func (p *ObjectRestoreTransformer) Execute(input *velero.RestoreItemActionExecuteInput) (*velero.RestoreItemActionExecuteOutput, error) {
	content := input.Item.UnstructuredContent()

	kind, ok := content["kind"].(string)
	if !ok {
		// cannot get resource kind string
		return velero.NewRestoreItemActionExecuteOutput(input.Item), fmt.Errorf("resource kind field error")
	}

	switch kind {
	case "StorageClass":
		sc, err := p.TransformStorageClass(content)
		if err != nil {
			p.log.Errorf("transform storageclass error: %v", err)
			return velero.NewRestoreItemActionExecuteOutput(input.Item), errors.WithStack(err)
		}

		if sc != nil {
			content, err = runtime.DefaultUnstructuredConverter.ToUnstructured(sc)
			if err != nil {
				return velero.NewRestoreItemActionExecuteOutput(input.Item), errors.WithStack(err)
			}
		}

		if name, exist, _ := unstructured.NestedString(content, "metadata", "name"); exist {
			p.log.Infof("StorageClass %v is transformed", name)
		}

	case "PersistentVolume":
		pv, err := p.TransformPersistentVolume(content)
		if err != nil {
			p.log.Errorf("transform pv error: %v", err)
			return velero.NewRestoreItemActionExecuteOutput(input.Item), errors.WithStack(err)
		}

		if pv != nil {
			content, err = runtime.DefaultUnstructuredConverter.ToUnstructured(pv)
			if err != nil {
				return velero.NewRestoreItemActionExecuteOutput(input.Item), errors.WithStack(err)
			}
		}

		if name, exist, _ := unstructured.NestedString(content, "metadata", "name"); exist {
			p.log.Infof("PersistentVolume %v is transformed", name)
		}

	default:
		// check every image field and make replacement
		if err := OperateObjectFields(content, "image", p.conf.ImageSource, ImageTransform); err != nil {
			p.log.Errorf("transform image failed: %v", err)
			return velero.NewRestoreItemActionExecuteOutput(input.Item), fmt.Errorf("transform image failed: %v", err)
		}
	}

	return velero.NewRestoreItemActionExecuteOutput(&unstructured.Unstructured{Object: content}), nil
}

// load or reload the configuration of transformation
func (p *ObjectRestoreTransformer) LoadConfig() error {
	// loading config of transformation
	var config TransformConfig

	if configFile, err := os.Open("/transformConf/transform-config.json"); os.IsNotExist(err) {
	} else if err != nil {
		return fmt.Errorf("cannot open /transformConf/transform-config.json: %v", err)
	} else {
		if data, err := ioutil.ReadAll(configFile); err != nil {
			return fmt.Errorf("read /transformConf/transform-config.json: %v", err)
		} else {
			if err = json.Unmarshal(data, &config); err != nil {
				return fmt.Errorf("Unmarshal /transformConf/PersistentVolume.json err: %v", err)
			} else {
				p.conf = config
			}
		}
	}
	return nil
}

// transform content to a PersistentVolume on alibabacloud ACK and return the pv
func (p *ObjectRestoreTransformer) TransformPersistentVolume(content map[string]interface{}) (interface{}, error) {
	pv := new(v1.PersistentVolume)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(content, pv); err != nil {
		return nil, errors.WithStack(err)
	}

	pvConf, exist := p.conf.PersistentVolume[pv.Name]
	if !exist {
		return nil, nil
	}

	vtype, exist := pvConf["transform_to"]
	if !exist {
		return nil, fmt.Errorf("transform_to field of volume config need to be set")
	}

	switch vtype {
	case "OSS":
		if err := createOSSVolume(pv, pvConf["bucket"], pvConf["url"], pvConf["akId"], pvConf["akSecret"], pvConf["otherOpts"]); err != nil {
			return nil, err
		}
	case "DISK":
		if err := createDiskVolume(pv, pvConf["region"], pvConf["zone"], pvConf["fsType"], pvConf["volumeId"]); err != nil {
			return nil, err
		}
	case "NAS":
		if err := createNASVolume(pv, pvConf["server"], pvConf["path"], pvConf["vers"]); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported type of volume config: %v", vtype)
	}
	return pv, nil
}

// transform content to a StorageClass on alibabacloud ACK and return the sc
func (p *ObjectRestoreTransformer) TransformStorageClass(content map[string]interface{}) (interface{}, error) {
	sc := new(storagev1.StorageClass)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(content, sc); err != nil {
		return nil, errors.WithStack(err)
	}

	scConf, exist := p.conf.StorageClass[sc.Name]
	if !exist {
		return nil, nil
	}

	provisioner, exist := scConf["alibaba_ACK_provisioner"]
	if !exist {
		return nil, fmt.Errorf("alibaba_ACK_provisioner field of volume config need to be set")
	}

	switch provisioner {
	case "DISK":
		if err := createDiskStorageClass(sc, scConf["type"], scConf["region"], scConf["zone"], scConf["fstype"], scConf["readonly"], scConf["encrypted"]); err != nil {
			return nil, err
		}
		return sc, nil

	case "NAS":
		mopts := strings.Split(scConf["mountOptions"], ",")
		for index, opt := range mopts {
			mopts[index] = strings.TrimSpace(opt)
		}
		if err := createNASStorageClass(sc, scConf["drivertype"], scConf["nfsversion"], scConf["options"], mopts); err != nil {
			return nil, err
		}
		return sc, nil

	default:
		return nil, fmt.Errorf("unsupported provisioner of storageclass config: %v", provisioner)
	}
}

// create an alibabacloud ACK OSS volume
func createOSSVolume(pv *v1.PersistentVolume, bucket string, url string, akId string, akSecret string, otherOpts string) error {
	if bucket == "" || url == "" || akId == "" || akSecret == "" {
		return fmt.Errorf("alibabacloud OSS volume config is invalid. bucket, url, akId, akSecrete should not be empty")
	}

	removePersistentVolumeSource(pv)

	pv.Spec.FlexVolume.Driver = "alicloud/oss"
	pv.Spec.FlexVolume.Options["bucket"] = bucket
	pv.Spec.FlexVolume.Options["url"] = url
	pv.Spec.FlexVolume.Options["akId"] = akId
	pv.Spec.FlexVolume.Options["akSecret"] = akSecret
	pv.Spec.FlexVolume.Options["otherOpts"] = otherOpts

	return nil
}

// create an alibabacloud ACK Disk volume
func createDiskVolume(pv *v1.PersistentVolume, region string, zone string, fsType string, volumeId string) error {
	if region == "" || zone == "" || fsType == "" || volumeId == "" {
		return fmt.Errorf("alibabacloud Disk volume config is invalid. region, zone, fsType should not be empty")
	}

	if fsType == "ext4" || fsType == "ext3" || fsType == "xfs" || fsType == "vfat" {
		removePersistentVolumeSource(pv)

		pv.SetLabels(map[string]string{
			"failure-domain.beta.kubernetes.io/zone":   zone,
			"failure-domain.beta.kubernetes.io/region": region,
		})
		pv.Spec.FlexVolume.Driver = "alicloud/disk"
		pv.Spec.FlexVolume.FSType = fsType
		pv.Spec.FlexVolume.Options["volumeId"] = volumeId

		return nil
	} else {
		return fmt.Errorf("unsupported alibabacloud Disk volume fsType: %v", fsType)
	}
}

// create an alibabacloud ACK NAS volume
func createNASVolume(pv *v1.PersistentVolume, server string, path string, vers string) error {
	if server == "" || path == "" {
		return fmt.Errorf("alibabacloud NAS volume config is invalid. server, path, vers should not be empty")
	}
	if vers != "3" && vers != "4.0" && vers != "" {
		return fmt.Errorf("unsupported nfs version %v, \"3\" or \"4.0\" are supported ", vers)
	}

	removePersistentVolumeSource(pv)

	pv.Spec.FlexVolume.Driver = "alicloud/nas"
	pv.Spec.FlexVolume.Options["server"] = server
	pv.Spec.FlexVolume.Options["path"] = path
	if vers != "" {
		pv.Spec.FlexVolume.Options["vers"] = vers
	}

	return nil
}

// create an alibabacloud Disk StorageClass
func createDiskStorageClass(sc *storagev1.StorageClass, sctype string, region string, zone string, fstype string, readonly string, encrypted string) error {
	if sctype == "" {
		return fmt.Errorf("alibabacloud NAS storageclass config is invalid. type should not be empty")
	}

	if sctype == "cloud_ssd" || sctype == "cloud" || sctype == "cloud_efficiency" || sctype == "available" {
		sc.Provisioner = "alicloud/disk"
		sc.Parameters = make(map[string]string)

		// Not validated, mount of the PVs will simply fail if one is invalid
		sc.MountOptions = []string{}
		sc.Parameters["type"] = sctype

		if region != "" && zone != "" {
			sc.Parameters["regionid"] = region
			sc.Parameters["zoneid"] = zone
		}
		if fstype != "" {
			sc.Parameters["fstype"] = fstype
		}
		if readonly != "" {
			sc.Parameters["readonly"] = readonly
		}
		if encrypted != "" {
			sc.Parameters["encrypted"] = encrypted
		}

		// Recycle is not supported by alibabacloud disk
		if *sc.ReclaimPolicy == v1.PersistentVolumeReclaimRecycle {
			*sc.ReclaimPolicy = v1.PersistentVolumeReclaimRetain
		}
		return nil
	} else {
		return fmt.Errorf("unsupported alibabacloud Disk StorageClass type: %v", sctype)
	}
}

// create an alibabacloud NAS StorageClass
func createNASStorageClass(sc *storagev1.StorageClass, drivertype string, nfsversion string, options string, mountOptions []string) error {
	if drivertype == "nfs" {
		if len(mountOptions) == 0 {
			return fmt.Errorf("mountOptions field should not be empty")
		}

		sc.Parameters = make(map[string]string)
		sc.MountOptions = mountOptions
	} else if drivertype == "flexvolume" {
		if nfsversion != "3" && nfsversion != "4.0" && nfsversion != "" {
			return fmt.Errorf("supported nfs version are \"3\" or \"4.0\", %v is not one of them", nfsversion)
		}

		sc.Parameters = make(map[string]string)
		sc.MountOptions = []string{}
		sc.Parameters["drivertype"] = "flexvolume"
		if nfsversion != "" {
			sc.Parameters["nfsversion"] = nfsversion
		}
		sc.Parameters["options"] = options
	} else {
		return fmt.Errorf("drivertype %v is illegal for alibabacloud ACK NAS StorageClass", drivertype)
	}
	sc.Provisioner = "alicloud/nas"
	return nil
}

// remove the fields of PersistentVolumeSource in PersistentVolumeSpec
// and initialize FlexVolume field
func removePersistentVolumeSource(pv *v1.PersistentVolume) {
	if pv != nil {
		pv.Spec.PersistentVolumeSource = v1.PersistentVolumeSource{}
		pv.Spec.FlexVolume = new(v1.FlexPersistentVolumeSource)
		pv.Spec.FlexVolume.Options = make(map[string]string)

		// Not validated, mount of the PVs will simply fail if one is invalid
		pv.Spec.MountOptions = []string{}
	}
}

// replace the deplicated image source in the image string with a new one
// according to sourceMap (map[string]string), return the new image string
func ImageTransform(sourceMap interface{}, image interface{}) (interface{}, error) {
	var imageString string
	var imageSourceMap map[string]string
	var ok bool

	if imageString, ok = image.(string); !ok {
		return image, fmt.Errorf("cannot change the image as it's not a string")
	}
	if imageSourceMap, ok = sourceMap.(map[string]string); !ok {
		return image, fmt.Errorf("images source map should be a map[string]string")
	}

	newImageString := imageString
	for old, new := range imageSourceMap {
		newImageString = strings.Replace(newImageString, old, new, 1)
	}

	return newImageString, nil
}

// traverse all the fileds of the (json) obj and exec operation in each field
// which name is field_name, replace value of the field with result of operation
// op_arg is the first parameter of the operation function,
// the second parameter of operation is the old value of each target filed
func OperateObjectFields(obj map[string]interface{}, field_name string, op_arg interface{}, operation func(interface{}, interface{}) (interface{}, error)) error {
	if obj == nil {
		return nil
	}

	for field, _ := range obj {
		if field == field_name {
			if result, err := operation(op_arg, obj[field]); err != nil {
				return err
			} else {
				obj[field] = result
			}
		} else if m, ok := obj[field].(map[string]interface{}); ok {
			if err := OperateObjectFields(m, field_name, op_arg, operation); err != nil {
				return err
			}
		} else if m, ok := obj[field].([]interface{}); ok {
			for _, item := range m {
				if n, ok := item.(map[string]interface{}); ok {
					if err := OperateObjectFields(n, field_name, op_arg, operation); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
