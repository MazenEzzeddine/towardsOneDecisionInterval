import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;


public class Controller implements Runnable {

    private static final Logger log = LogManager.getLogger(Controller.class);

    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Long sleep;
    static double doublesleep;
    static String topic;
    static String cluster;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    static Map<TopicPartition, OffsetAndMetadata> committedOffsets;

    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    ///////////////////////////////////////////////////////////////////////////
    static Instant lastUpScaleDecision;
    static Instant lastDownScaleDecision;
    static boolean firstIteration = true;

    static TopicDescription td;
    static DescribeTopicsResult tdr;
    static ArrayList<Partition> partitions = new ArrayList<>();

    static double dynamicTotalMaxConsumptionRate = 0.0;
    static double dynamicAverageMaxConsumptionRate = 0.0;
    static double wsla = 5.0;

    static List<Consumer> assignment;
    static Instant lastScaleUpDecision;
    static Instant lastScaleDownDecision;
    static int cgSize = 1;


    private static void readEnvAndCrateAdminClient() throws ExecutionException, InterruptedException {
        sleep = Long.valueOf(System.getenv("SLEEP"));
        topic = System.getenv("TOPIC");
        cluster = System.getenv("CLUSTER");
        poll = Long.valueOf(System.getenv("POLL"));
        CONSUMER_GROUP = System.getenv("CONSUMER_GROUP");
        BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        admin = AdminClient.create(props);
        tdr = admin.describeTopics(Collections.singletonList(topic));
        td = tdr.values().get(topic).get();

        lastScaleUpDecision = Instant.now();
        lastScaleDownDecision = Instant.now();


        for (TopicPartitionInfo p : td.partitions()) {
            partitions.add(new Partition(p.partition(), 0, 0));
        }
        log.info("topic has the following partitions {}", td.partitions().size());
    }


    private static void getCommittedLatestOffsetsAndLag() throws ExecutionException, InterruptedException {
        committedOffsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets1 = new HashMap<>();
        Map<TopicPartition, OffsetSpec> requestTimestampOffsets2 = new HashMap<>();


        // log.info("Date(System.currentTimeMillis()) {}",new Date(System.currentTimeMillis()));
        for (TopicPartitionInfo p : td.partitions()) {
            requestLatestOffsets.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
           /* requestTimestampOffsets2.put(new TopicPartition(topic, p.partition()),
           OffsetSpec.forTimestamp(Instant.now().minusMillis(1100).toEpochMilli()));
            requestTimestampOffsets1.put(new TopicPartition(topic, p.partition()),
             OffsetSpec.forTimestamp(Instant.now().minusMillis(sleep + 1100).toEpochMilli()));*/
            requestTimestampOffsets2.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(1500).toEpochMilli()));
            requestTimestampOffsets1.put(new TopicPartition(topic, p.partition()),
                    OffsetSpec.forTimestamp(Instant.now().minusMillis(sleep + 1500).toEpochMilli()));
        }

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                admin.listOffsets(requestLatestOffsets).all().get();

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets1 =
                admin.listOffsets(requestTimestampOffsets1).all().get();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> timestampOffsets2 =
                admin.listOffsets(requestTimestampOffsets2).all().get();


        long totalArrivalRate = 0;
        double currentPartitionArrivalRate = 0;
        Map<Integer, Double> previousPartitionArrivalRate = new HashMap<>();
        for (TopicPartitionInfo p : td.partitions()) {
            previousPartitionArrivalRate.put(p.partition(), 0.0);
        }

        for (TopicPartitionInfo p : td.partitions()) {
            TopicPartition t = new TopicPartition(topic, p.partition());
            long latestOffset = latestOffsets.get(t).offset();
            long timeoffset1 = timestampOffsets1.get(t).offset();
            long timeoffset2 = timestampOffsets2.get(t).offset();

            if (timeoffset2 == -1) {
                timeoffset2 = latestOffset;
            }
            if (timeoffset1 == -1) {
                // NOT very critical condition
                currentPartitionArrivalRate = previousPartitionArrivalRate.get(p.partition());
                log.info("Arrival rate into partition {} is {}", t.partition(), currentPartitionArrivalRate);

            } else {
                currentPartitionArrivalRate = (double) (timeoffset2 - timeoffset1) / doublesleep;
                log.info(" timeoffset1 {}, timeoffset2 {}", timeoffset1, timeoffset2);
                log.info("Arrival rate into partition {} is {}", t.partition(), currentPartitionArrivalRate);
            }
            //TODO add a condition for when both offsets timeoffset2 and timeoffset1 do not exist, i.e., are -1,
            previousPartitionArrivalRate.put(p.partition(), currentPartitionArrivalRate);
            totalArrivalRate += currentPartitionArrivalRate;

        }
        log.info("totalArrivalRate {}", totalArrivalRate);


        if (!firstIteration) {
            //computeTotalArrivalRate();
            youMightWanttoScale(totalArrivalRate);
        } else {
            firstIteration = false;
        }
    }


    private static void youMightWanttoScale(double totalArrivalRate) throws ExecutionException, InterruptedException {
        log.info("Inside you youMightWanttoScale");
        //int size = consumerGroupDescriptionMap.get(Controller.CONSUMER_GROUP).members().size();
        int size = cgSize;
        log.info("curent group size is {}", size);

        if (Duration.between(lastUpScaleDecision, Instant.now()).toSeconds() >= 15) {
            log.info("Upscale logic, Up scale cool down has ended");

            upScaleLogic(totalArrivalRate, size);
        } else {
            log.info("Not checking  upscale logic, Up scale cool down has not ended yet");
        }


        if (Duration.between(lastDownScaleDecision, Instant.now()).toSeconds() >= 30) {
            log.info("DownScaling logic, Down scale cool down has ended");
            downScaleLogic(totalArrivalRate, size);
        } else {
            log.info("Not checking  down scale logic, down scale cool down has not ended yet");
        }
    }


    private static void upScaleLogic(double totalArrivalRate, int size) {
        if ((totalArrivalRate) > size * poll) {
            log.info("Consumers are less than nb partition we can scale");

            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {

                k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(size + 1);


                log.info("Since  arrival rate {} is greater than  maximum consumption rate " +
                        "{} ,  I up scaled  by one ", totalArrivalRate, size * poll);
            }
            cgSize++;
            lastUpScaleDecision = Instant.now();
            lastDownScaleDecision = Instant.now();
        }
    }


    private static void downScaleLogic(double totalArrivalRate, int size) {
        if ((totalArrivalRate < (size - 1) * poll) && totalArrivalRate != 0) {

            log.info("since  arrival rate {} is lower than maximum consumption rate " +
                            " with size - 1  I down scaled  by one {}",
                    totalArrivalRate * 1000, size * poll);
            try (final KubernetesClient k8s = new DefaultKubernetesClient()) {

                // is that necessary

                int replicas = k8s.apps().deployments().inNamespace("default").withName("cons1persec").get().getSpec().getReplicas();
                if (replicas > 1) {
                    k8s.apps().deployments().inNamespace("default").withName("cons1persec").scale(replicas - 1);
                    lastDownScaleDecision = Instant.now();
                    lastUpScaleDecision = Instant.now();
                    cgSize--;

                } else {
                    log.info("Not going to  down scale since replicas already one");
                }
            }
        }
    }


    @Override
    public void run() {
        try {
            readEnvAndCrateAdminClient();
            log.info("sleep is {}", sleep);
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        lastUpScaleDecision = Instant.now();
        lastDownScaleDecision = Instant.now();

        doublesleep = (double) sleep / 1000.0;


        while (true) {
            log.info("New Iteration:");
            try {
                getCommittedLatestOffsetsAndLag();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("Sleeping for {} seconds", doublesleep);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            log.info("End Iteration;");
            log.info("=============================================");
        }
    }
}




