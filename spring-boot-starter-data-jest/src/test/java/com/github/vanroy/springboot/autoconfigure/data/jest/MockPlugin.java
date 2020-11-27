package com.github.vanroy.springboot.autoconfigure.data.jest;

import org.elasticsearch.plugins.Plugin;

public class MockPlugin extends Plugin {

    public static int instancesCount = 0;

    public MockPlugin() {
        instancesCount++;
    }
}
