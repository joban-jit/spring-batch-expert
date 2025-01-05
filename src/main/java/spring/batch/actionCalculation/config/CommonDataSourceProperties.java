package spring.batch.actionCalculation.config;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.PropertySource;


public class CommonDataSourceProperties extends DataSourceProperties {
//    private String url;
//    private String username;
//    private String password;
    // above already exist in DataSourceProperties class

    private String driverClassName;
    private String poolName;

    // getters and setters

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }
}
