package io.axoniq.axonserver.enterprise.taskscheduler.task;

import java.io.Serializable;

public class UpdateLicenceTaskPayload implements Serializable {

    private String nodeName;
    private byte[] licencePayload;

    public UpdateLicenceTaskPayload(){

    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setLicencePayload(byte[] licencePayload) {
        this.licencePayload = licencePayload;
    }

    public UpdateLicenceTaskPayload(String nodeName, byte[] licencePayload) {
        this.nodeName = nodeName;
        this.licencePayload = licencePayload;
    }

    public String getNodeName() {
        return nodeName;
    }

    public byte[] getLicencePayload() {
        return licencePayload;
    }


}
