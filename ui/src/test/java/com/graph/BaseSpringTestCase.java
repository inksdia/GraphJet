package com.graph;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.AbstractJUnit4SpringContextTests;

/**
 * Created by saurav on 28/03/17.
 */
@ContextConfiguration(locations = "classpath*:/spring/spring*.xml")
public class BaseSpringTestCase extends AbstractJUnit4SpringContextTests {

    @Autowired
    protected ApplicationContext applicationContext;

}
