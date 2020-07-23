package io.axoniq.axonserver.enterprise.taskscheduler.task;

import io.axoniq.axonserver.KeepNames;

import java.io.Serializable;

@KeepNames
public class UpdateLicenseTaskPayload implements Serializable {

    private String nodeName;
    private byte[] licensePayload;

    public UpdateLicenseTaskPayload(){

    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setLicensePayload(byte[] licensePayload) {
        this.licensePayload = licensePayload;
    }

    public UpdateLicenseTaskPayload(String nodeName, byte[] licensePayload) {
        this.nodeName = nodeName;
        this.licensePayload = licensePayload;
    }

    public String getNodeName() {
        return nodeName;
    }

    public byte[] getLicensePayload() {
        return licensePayload;
    }
}
