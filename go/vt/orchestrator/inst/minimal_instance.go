package inst

type MinimalInstance struct {
	Key         InstanceKey
	PrimaryKey  InstanceKey
	ClusterName string
}

func (minimalInstance *MinimalInstance) ToInstance() *Instance {
	return &Instance{
		Hostname:    minimalInstance.Key.Hostname,
		Port:        minimalInstance.Key.Port,
		SourceHost:  minimalInstance.PrimaryKey.Hostname,
		SourcePort:  minimalInstance.PrimaryKey.Port,
		ClusterName: minimalInstance.ClusterName,
	}
}
