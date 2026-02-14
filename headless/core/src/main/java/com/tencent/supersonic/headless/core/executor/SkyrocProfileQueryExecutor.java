package com.tencent.supersonic.headless.core.executor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.tencent.supersonic.common.util.HttpUtils;
import com.tencent.supersonic.headless.api.pojo.response.DatabaseResp;
import com.tencent.supersonic.headless.api.pojo.response.SemanticQueryResp;
import com.tencent.supersonic.headless.core.pojo.QueryStatement;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * 将查询执行账号切换为 Skyroc 侧按用户角色生成的 Doris Profile 账号。
 *
 * 环境变量：
 * - SKYROC_BASE_URL: Skyroc 后端地址，例如 http://127.0.0.1:8000
 * - SKYROC_INTERNAL_TOKEN: Skyroc 内部接口鉴权 token（对应 Skyroc SUPERSONIC_INTERNAL_TOKEN）
 */
@Slf4j
public class SkyrocProfileQueryExecutor implements QueryExecutor {

    private static final String ENV_BASE_URL = "SKYROC_BASE_URL";
    private static final String ENV_INTERNAL_TOKEN = "SKYROC_INTERNAL_TOKEN";
    private static final String INTERNAL_TOKEN_HEADER = "X-Skyroc-Internal-Token";
    private static final String PROFILE_ENDPOINT_PATH = "/internal/doris/profile-credentials";

    private final JdbcExecutor delegate = new JdbcExecutor();

    @Override
    public boolean accept(QueryStatement queryStatement) {
        if (queryStatement == null || queryStatement.getUser() == null) {
            return false;
        }
        if (queryStatement.getOntology() == null || queryStatement.getOntology().getDatabase() == null) {
            return false;
        }
        return isEnabled();
    }

    @Override
    public SemanticQueryResp execute(QueryStatement queryStatement) {
        try {
            DatabaseResp database = queryStatement.getOntology().getDatabase();

            String userName = queryStatement.getUser().getName();
            if (userName == null || userName.isBlank()) {
                return delegate.execute(queryStatement);
            }

            Map<String, Object> body = new HashMap<>();
            body.put("userName", userName);

            Map<String, String> headers = new HashMap<>();
            headers.put(INTERNAL_TOKEN_HEADER, internalToken());

            String url = normalizeBaseUrl(baseUrl()) + PROFILE_ENDPOINT_PATH;
            String respText = HttpUtils.post(url, body, headers);
            JSONObject resp = JSON.parseObject(respText);
            if (resp == null || !"0000".equals(String.valueOf(resp.get("code")))) {
                log.warn("Skyroc profile credentials request failed, resp={}", respText);
                return delegate.execute(queryStatement);
            }

            JSONObject data = resp.getJSONObject("data");
            if (data == null) {
                log.warn("Skyroc profile credentials missing data, resp={}", respText);
                return delegate.execute(queryStatement);
            }

            String profileUser = data.getString("userName");
            String profilePassword = data.getString("password");
            if (profileUser == null || profileUser.isBlank() || profilePassword == null || profilePassword.isBlank()) {
                log.warn("Skyroc profile credentials invalid, resp={}", respText);
                return delegate.execute(queryStatement);
            }

            database.setUsername(profileUser);
            // 允许明文；Supersonic 的 passwordDecrypt 失败时会回退返回原值。
            database.setPassword(profilePassword);
        } catch (Exception e) {
            log.warn("Skyroc profile executor fallback to default, err={}", e.getMessage());
        }

        return delegate.execute(queryStatement);
    }

    private static boolean isEnabled() {
        String base = baseUrl();
        String token = internalToken();
        return base != null && !base.isBlank() && token != null && !token.isBlank();
    }

    private static String baseUrl() {
        return System.getenv(ENV_BASE_URL);
    }

    private static String internalToken() {
        return System.getenv(ENV_INTERNAL_TOKEN);
    }

    private static String normalizeBaseUrl(String baseUrl) {
        if (baseUrl == null) {
            return "";
        }
        String trimmed = baseUrl.trim();
        if (trimmed.endsWith("/")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
