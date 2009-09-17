package org.gparallelizer.remote;

import org.gparallelizer.remote.messages.HostIdMsg;
import org.gparallelizer.serial.AbstractMsg;

/**
 * Represents connection to remote host
 *
 * @author Alex Tkachman
 */
public abstract class RemoteConnection {
    private final LocalHost localHost;

    private RemoteHost host;

    public RemoteConnection(LocalHost provider) {
        this.localHost = provider;
    }

    public void onMessage(AbstractMsg msg) {
        if (host == null) {
            final HostIdMsg idMsg = (HostIdMsg) msg;
            host = (RemoteHost) localHost.getSerialHost(idMsg.hostId, this);
        } else
            throw new IllegalStateException("Unexpected message: " + msg);
    }

    public void onException(Throwable cause) {
    }

    public void onConnect() {
        write(new HostIdMsg(localHost.getId()));
    }

    public void onDisconnect() {
        localHost.onDisconnect(host);
    }

    public abstract void write(AbstractMsg msg);

    public RemoteHost getHost() {
        return host;
    }

    public void setHost(RemoteHost host) {
        this.host = host;
    }

    public abstract void disconnect();
}