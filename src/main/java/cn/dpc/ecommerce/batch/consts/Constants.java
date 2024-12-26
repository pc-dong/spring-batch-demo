package cn.dpc.ecommerce.batch.consts;

import java.util.List;

public class Constants {
    private Constants() {
    }

    public static final String LAST_UPDATE_TIME = "lastUpdateTime";
    public static final String END_TIME = "endTime";
    public static final String APP_NAME = "appName";
    public static final String TYPE = "type";
    public static final String UPDATED_UPDATE_TIME = "updatedUpdateTime";
    public static final String NULL_UUID = "0";
    public static final long NULL_ID = 0;
    public static final List<String> activePropertyStatus = List.of("ONLINE", "PRE-SALE");
}
