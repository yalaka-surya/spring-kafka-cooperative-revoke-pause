plugins {
    java
}

// The behaviour under test is in spring-kafka's KafkaMessageListenerContainer, so the version is
// the only thing worth varying:  ./gradlew test -PspringKafkaVersion=4.0.7
// 4.1.1 is the newest release at the time of writing; the same code is present on main
// (4.2.0-SNAPSHOT) and in the 3.3.x line.
val springKafkaVersion: String = providers.gradleProperty("springKafkaVersion").getOrElse("4.1.1")

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
    // Only for running against a spring-kafka you built yourself:
    //   (in a spring-kafka checkout)  ./gradlew :spring-kafka:publishToMavenLocal
    //   (here)                        ./gradlew test -PspringKafkaVersion=4.2.0-SNAPSHOT
    // main tracks Spring Framework and Micrometer snapshots, hence the extra repositories.
    if (springKafkaVersion.endsWith("SNAPSHOT")) {
        mavenLocal()
        maven { url = uri("https://repo.spring.io/snapshot") }
        maven { url = uri("https://repo.spring.io/milestone") }
    }
}

dependencies {
    // Pulls kafka-clients transitively; MockConsumer comes from there.
    testImplementation("org.springframework.kafka:spring-kafka:$springKafkaVersion")

    testImplementation(platform("org.junit:junit-bom:5.12.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.27.7")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.32")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = true
    }
    doFirst {
        logger.lifecycle("Running against spring-kafka $springKafkaVersion")
    }
}
