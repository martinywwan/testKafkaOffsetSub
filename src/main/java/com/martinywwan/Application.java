package com.martinywwan;

import com.martinywwan.launcher.KafkaLauncher;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan
public class Application {

    public static void main(String[] args) {
        ApplicationContext ac = new AnnotationConfigApplicationContext(Application.class);
        KafkaLauncher kl = ac.getBean(KafkaLauncher.class);
        kl.call();
    }
}
