package org.prwatech.kafka.serializers.protobuff;

import org.prwatech.kafka.beans.CompanyResources;

public class ProtoDataSetup {

    public static CompanyResources.Employee createEmployee(int id, String name) {
        return CompanyResources.Employee.newBuilder()
                .setEmployeeId(id)
                .setEmployeeName(name)
                .build();
    }
}
