package cn.zzs.serialize;

import cn.zzs.serialize.other.User;
import cn.zzs.serialize.other.UserService;
import com.alibaba.fastjson2.JSON;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.util.Pool;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.nustaq.serialization.FSTConfiguration;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.openjdk.jmh.annotations.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;

/**
 * 序列化性能对比测试
 *
 * @author zzs
 * @date 2020年11月6日 下午1:29:28
 */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SerializeTest {

    /**
     * 用来序列化的用户对象
     *
     * @author zzs
     * @date 2020年11月6日 下午4:11:06
     */
    @State(Scope.Benchmark)
    public static class CommonState {
        User user;

        @Setup(Level.Trial)
        public void prepare() {
            user = new UserService().get();
        }
    }

    /**
     * 测试fastjson序列化
     *
     * @param commonState
     * @return byte[]
     * @author zzs
     * @date 2020年11月6日 下午4:11:23
     */
    @Benchmark
    public byte[] fastJsonSerialize(CommonState commonState) {
        return JSON.toJSONBytes(commonState.user);
    }

    /**
     * 测试jackson序列化，ObjectMapper仅初始化一个实例，不重复创建
     *
     * @param commonState
     * @param jacksonState
     * @return byte[]
     * @throws Exception
     * @author zzs
     * @date 2020年11月6日 下午5:10:18
     */
    @Benchmark
    public byte[] jacksonSerialize(CommonState commonState, JacksonState jacksonState) throws Exception {
        return jacksonState.objectMapper.writeValueAsBytes(commonState.user);
    }

    @State(Scope.Benchmark)
    public static class JacksonState {
        ObjectMapper objectMapper;

        @Setup(Level.Trial)
        public void prepare() {
            objectMapper = new ObjectMapper();
        }
    }

    /**
     * 测试ObjectOutputStream序列化
     *
     * @param commonState
     * @return byte[]
     * @throws Exception
     * @author zzs
     * @date 2020年11月6日 下午4:12:53
     */
    @Benchmark
    public byte[] jdkSerialize(CommonState commonState) throws Exception {
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(byteArray);
        outputStream.writeObject(commonState.user);
        outputStream.flush();
        outputStream.close();
        return byteArray.toByteArray();
    }

    /**
     * 测试kryo序列化，Kryo对象不是线程安全的，这里使用池获取
     *
     * @param commonState
     * @param kryoState
     * @return byte[]
     * @author zzs
     * @date 2020年11月8日 下午2:50:20
     */
    @Benchmark
    public byte[] kryoSerialize(CommonState commonState, KryoState kryoState) {
        ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
        Output output = new Output(byteArray);
        Kryo kryo = kryoState.kryoPool.obtain();
        kryo.writeClassAndObject(output, commonState.user);
        kryoState.kryoPool.free(kryo);
        output.flush();
        output.close();
        return byteArray.toByteArray();
    }

    @State(Scope.Benchmark)
    public static class KryoState {
        Pool<Kryo> kryoPool;

        @Setup(Level.Trial)
        public void prepare() {
            kryoPool = new Pool<Kryo>(true, false, 16) {
                protected Kryo create() {
                    Kryo kryo = new Kryo();
                    // 不支持循环引用
                    kryo.setReferences(false);
                    // 禁止类注册
                    kryo.setRegistrationRequired(false);
                    // kryo.register(User.class);
                    // 设置实例化器
                    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
                    return kryo;
                }
            };
        }
    }

    /**
     * 测试fst序列化，FSTConfiguration仅初始化一个实例，不重复创建
     *
     * @param commonState
     * @param fSTConfigurationState
     * @return byte[]
     * @author zzs
     * @date 2020年11月8日 下午3:04:34
     */
    @Benchmark
    public byte[] fstSerialize(CommonState commonState, FSTConfigurationState fSTConfigurationState) {
        return fSTConfigurationState.fSTConfiguration.asByteArray(commonState.user);
    }

    @State(Scope.Benchmark)
    public static class FSTConfigurationState {
        FSTConfiguration fSTConfiguration;

        @Setup(Level.Trial)
        public void prepare() {
            //fSTConfiguration = FSTConfiguration.createJsonConfiguration();
            fSTConfiguration = FSTConfiguration.createDefaultConfiguration();
        }
    }

    /**
     * 测试protostuff序列化
     *
     * @param commonStatem
     * @param protostuffState
     * @return byte[]
     * @author zzs
     * @date 2020年11月8日 下午3:06:07
     */
    @Benchmark
    public byte[] protostuffSerialize(CommonState commonStateme) {
        Schema<User> schema = (Schema<User>) RuntimeSchema.getSchema(User.class);
        return ProtostuffIOUtil.toByteArray(commonStateme.user, schema, LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE));
    }

}
