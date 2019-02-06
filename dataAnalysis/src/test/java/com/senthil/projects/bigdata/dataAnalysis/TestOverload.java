package com.senthil.projects.bigdata.dataAnalysis;

import org.junit.jupiter.api.Test;

public class TestOverload {
    @Test
    public void findIntSquare(){
        System.out.println(square(10));
    }

    private long square(long l) { return l*l; }
}
