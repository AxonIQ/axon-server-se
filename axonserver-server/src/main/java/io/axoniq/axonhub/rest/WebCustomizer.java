package io.axoniq.axonhub.rest;

import io.axoniq.axonhub.KeepNames;
import io.axoniq.axonhub.connector.Page;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Author: marc
 */
@RestController("WebCustomizer")
@RequestMapping("/v1/public")
public class WebCustomizer {
    private final List<Page> pages;
    private final Logger logger = LoggerFactory.getLogger(WebCustomizer.class);

    public WebCustomizer(Optional<List<Page>> pages) {
        this.pages = pages.orElse(Collections.emptyList());
    }

    @GetMapping("pages")
    public List<PageDefinition> pages() {
        logger.info("Pages: {}", pages);
        return pages.stream().map(PageDefinition::new).collect(Collectors.toList());
    }

    @KeepNames
    public static class PageDefinition {
        private Page page;

        public PageDefinition(Page page) {
            this.page = page;
        }

        public String getUrl() {
            return page.getUrl();
        }

        public String getIconUrl() {
            return page.getIconUrl();
        }

        public String getTitle() {
            return page.getTitle();
        }
    }


}
