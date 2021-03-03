/*
 * Copyright (c) 2017-2019 AxonIQ B.V. and/or licensed to AxonIQ B.V.
 * under one or more contributor license agreements.
 *
 *  Licensed under the AxonIQ Open Source License Agreement v1.0;
 *  you may not use this file except in compliance with the license.
 *
 */

globals.pageView = new Vue(
        {el: '#overview',
            data: {
                component: null,
                context: null,
                title: null,
                activeContext: "_all",
                webSocketInfo: globals.webSocketInfo,
                contexts: ["default"]
            },
            mounted() {
                let me = this;
                me.webSocketInfo.subscribe('/topic/cluster', function () {
                    me.initOverview();
                }, function (sub) {
                    me.subscription = sub;
                });
                if (globals.isEnterprise()) {
                    axios.get("v1/public/visiblecontexts?includeAdmin=false").then(response => {
                        me.contexts = response.data;
                        me.initOverview();
                    });
                } else {
                    me.initOverview();
                }
            },
            beforeDestroy() {
                if( this.subscription) this.subscription.unsubscribe();
            },
            methods: {
                initOverview() {
                    let contextString = this.activeContext === "_all" ? "" : "?for-context=" + this.activeContext;
                    $.getJSON("v1/public/overview" + contextString, function (node) {
                        $("svg").attr("width", node.width).attr("height", node.height);
                        $("g").html(node.svgObjects);
                    });
                },

                selectComponent(component, context, title) {
                    this.component = component;
                    this.context = context;
                    this.title = title;
                },

                deselectComponent() {
                    this.component = null;
                    this.context = null;
                    this.title = null;
                }
            }
        });

var currentPopup;

function showArea(event, id, nodeType, context, title) {
    if (nodeType === 'client'){
        globals.pageView.selectComponent(id, context, title);
        return true;
    } else {
        if (currentPopup) hideArea(currentPopup);
        if (document.getElementById(id)) {
            document.getElementById(id).setAttributeNS(null, "visibility", "visible");
            currentPopup = id;
        }
        event.stopPropagation();
        return true;
    }
}

function hideArea(id) {
    console.log(id);
    document.getElementById(id).setAttributeNS(null, "visibility", "hidden");
    currentPopup = null;
}

function hidePopup() {
    if (currentPopup) hideArea(currentPopup);
}

