package ai.promoted.metrics.logprocessor.common.queryablestate;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class HostUtils {

  public static String getFirstNonLocalIp() {
    try {
      for (Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
          en.hasMoreElements(); ) {
        NetworkInterface intf = en.nextElement();
        for (Enumeration<InetAddress> enumIpAddr = intf.getInetAddresses();
            enumIpAddr.hasMoreElements(); ) {
          InetAddress address = enumIpAddr.nextElement();
          if (!address.isLoopbackAddress() && !address.isLinkLocalAddress()) {
            return address.getHostAddress();
          }
        }
      }
      return null;
    } catch (SocketException e) {
      throw new RuntimeException(e);
    }
  }
}
