package net.marcoreis.hadoop.util;

public class FileStatus {
    private String accessTime;
    private String blockSize;
    private String childrenNum;
    private String fileId;
    private String group;
    private long length;
    private long modificationTime;
    private String owner;
    private String pathSuffix;
    private int permission;
    private int replication;
    private int storagePolicy;
    private String type;

    public String getAccessTime() {
	return accessTime;
    }

    public void setAccessTime(String accessTime) {
	this.accessTime = accessTime;
    }

    public String getBlockSize() {
	return blockSize;
    }

    public void setBlockSize(String blockSize) {
	this.blockSize = blockSize;
    }

    public String getChildrenNum() {
	return childrenNum;
    }

    public void setChildrenNum(String childrenNum) {
	this.childrenNum = childrenNum;
    }

    public String getFileId() {
	return fileId;
    }

    public void setFileId(String fileId) {
	this.fileId = fileId;
    }

    public String getGroup() {
	return group;
    }

    public void setGroup(String group) {
	this.group = group;
    }

    public long getLength() {
	return length;
    }

    public void setLength(long length) {
	this.length = length;
    }

    public long getModificationTime() {
	return modificationTime;
    }

    public void setModificationTime(long modificationTime) {
	this.modificationTime = modificationTime;
    }

    public String getOwner() {
	return owner;
    }

    public void setOwner(String owner) {
	this.owner = owner;
    }

    public String getPathSuffix() {
	return pathSuffix;
    }

    public void setPathSuffix(String pathSuffix) {
	this.pathSuffix = pathSuffix;
    }

    public int getPermission() {
	return permission;
    }

    public void setPermission(int permission) {
	this.permission = permission;
    }

    public int getReplication() {
	return replication;
    }

    public void setReplication(int replication) {
	this.replication = replication;
    }

    public int getStoragePolicy() {
	return storagePolicy;
    }

    public void setStoragePolicy(int storagePolicy) {
	this.storagePolicy = storagePolicy;
    }

    public String getType() {
	return type;
    }

    public void setType(String type) {
	this.type = type;
    }
}
