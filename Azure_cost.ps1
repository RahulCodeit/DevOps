<!DOCTYPE html><html lang="en-US" class="" data-primer><head><link href="https://a.slack-edge.com/73ef1f2/marketing/style/onetrust/onetrust_banner.css" rel="stylesheet" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous"><link href="https://a.slack-edge.com/2e88f6d/style/libs/lato-2-compressed.css" rel="stylesheet" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous"><link href="https://a.slack-edge.com/97c9d7c/style/_generic.typography.larsseit.css" rel="stylesheet" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous"><link href="https://a.slack-edge.com/6482e2c/style/_generic.typography.avantgarde.css" rel="stylesheet" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous"><link href="https://a.slack-edge.com/c171f6d/style/_generic.typography.sfsans.css" rel="stylesheet" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous"><link rel="canonical" href="https://slack.com">

<link rel="alternate" hreflang="en-us" href="https://slack.com">

<link rel="alternate" hreflang="en-au" href="https://slack.com/intl/en-au">

<link rel="alternate" hreflang="en-gb" href="https://slack.com/intl/en-gb">

<link rel="alternate" hreflang="en-in" href="https://slack.com/intl/en-in">

<link rel="alternate" hreflang="fr-ca" href="https://slack.com/intl/fr-ca">

<link rel="alternate" hreflang="fr-fr" href="https://slack.com/intl/fr-fr">

<link rel="alternate" hreflang="de-de" href="https://slack.com/intl/de-de">

<link rel="alternate" hreflang="es-es" href="https://slack.com/intl/es-es">

<link rel="alternate" hreflang="es" href="https://slack.com/intl/es-la">

<link rel="alternate" hreflang="ja-jp" href="https://slack.com/intl/ja-jp">

<link rel="alternate" hreflang="pt-br" href="https://slack.com/intl/pt-br">

<link rel="alternate" hreflang="ko-kr" href="https://slack.com/intl/ko-kr">

<link rel="alternate" hreflang="it-it" href="https://slack.com/intl/it-it">

<link rel="alternate" hreflang="zh-cn" href="https://slack.com/intl/zh-cn">

<link rel="alternate" hreflang="zh-tw" href="https://slack.com/intl/zh-tw">

<link rel="alternate" hreflang="x-default" href="https://slack.com">

<script>window.ts_endpoint_url = "https:\/\/slack.com\/beacon\/timing";(function(e) {
	var n=Date.now?Date.now():+new Date,r=e.performance||{},t=[],a={},i=function(e,n){for(var r=0,a=t.length,i=[];a>r;r++)t[r][e]==n&&i.push(t[r]);return i},o=function(e,n){for(var r,a=t.length;a--;)r=t[a],r.entryType!=e||void 0!==n&&r.name!=n||t.splice(a,1)};r.now||(r.now=r.webkitNow||r.mozNow||r.msNow||function(){return(Date.now?Date.now():+new Date)-n}),r.mark||(r.mark=r.webkitMark||function(e){var n={name:e,entryType:"mark",startTime:r.now(),duration:0};t.push(n),a[e]=n}),r.measure||(r.measure=r.webkitMeasure||function(e,n,r){n=a[n].startTime,r=a[r].startTime,t.push({name:e,entryType:"measure",startTime:n,duration:r-n})}),r.getEntriesByType||(r.getEntriesByType=r.webkitGetEntriesByType||function(e){return i("entryType",e)}),r.getEntriesByName||(r.getEntriesByName=r.webkitGetEntriesByName||function(e){return i("name",e)}),r.clearMarks||(r.clearMarks=r.webkitClearMarks||function(e){o("mark",e)}),r.clearMeasures||(r.clearMeasures=r.webkitClearMeasures||function(e){o("measure",e)}),e.performance=r,"function"==typeof define&&(define.amd||define.ajs)&&define("performance",[],function(){return r}) // eslint-disable-line
})(window);</script><script>

(function () {
	
	window.TSMark = function (mark_label) {
		if (!window.performance || !window.performance.mark) return;
		performance.mark(mark_label);
	};
	window.TSMark('start_load');

	
	window.TSMeasureAndBeacon = function (measure_label, start_mark_label) {
		if (!window.performance || !window.performance.mark || !window.performance.measure) {
			return;
		}

		performance.mark(start_mark_label + '_end');

		try {
			performance.measure(measure_label, start_mark_label, start_mark_label + '_end');
			window.TSBeacon(measure_label, performance.getEntriesByName(measure_label)[0].duration);
		} catch (e) {
			
		}
	};

	
	if ('sendBeacon' in navigator) {
		window.TSBeacon = function (label, value) {
			var endpoint_url = window.ts_endpoint_url || 'https://slack.com/beacon/timing';
			navigator.sendBeacon(
				endpoint_url + '?data=' + encodeURIComponent(label + ':' + value),
				'',
			);
		};
	} else {
		window.TSBeacon = function (label, value) {
			var endpoint_url = window.ts_endpoint_url || 'https://slack.com/beacon/timing';
			new Image().src = endpoint_url + '?data=' + encodeURIComponent(label + ':' + value);
		};
	}
})();
</script><script>window.TSMark('step_load');</script><script>
(function () {
	function throttle(callback, intervalMs) {
		var wait = false;

		return function () {
			if (!wait) {
				callback.apply(null, arguments);
				wait = true;
				setTimeout(function () {
					wait = false;
				}, intervalMs);
			}
		};
	}

	function getGenericLogger() {
		return {
			warn: (msg) => {
				
				if (window.console && console.warn) return console.warn(msg);
			},
			error: (msg) => {
				if (!msg) return;

				if (window.TSBeacon) return window.TSBeacon(msg, 1);

				
				if (window.console && console.warn) return console.warn(msg);
			},
		};
	}

	function globalErrorHandler(evt) {
		if (!evt) return;

		
		var details = '';

		var node = evt.srcElement || evt.target;

		var genericLogger = getGenericLogger();

		
		
		
		
		if (node && node.nodeName) {
			var nodeSrc = '';
			if (node.nodeName === 'SCRIPT') {
				nodeSrc = node.src || 'unknown';
				var errorType = evt.type || 'error';

				
				details = `[${errorType}] from script at ${nodeSrc} (failed to load?)`;
			} else if (node.nodeName === 'IMG') {
				nodeSrc = node.src || node.currentSrc;

				genericLogger.warn(`<img> fired error with url = ${nodeSrc}`);
				return;
			}
		}

		
		if (evt.error && evt.error.stack) {
			details += ` ${evt.error.stack}`;
		}

		if (evt.filename) {
			
			var fileName = evt.filename;
			var lineNo = evt.lineno;
			var colNo = evt.colno;

			details += ` from ${fileName}`;

			
			if (lineNo) {
				details += ` @ line ${lineNo}, col ${colNo}`;
			}
		}

		var msg;

		
		if (evt.error && evt.error.stack) {
			
			msg = details;
		} else {
			
			msg = `${evt.message || ''} ${details}`;
		}

		
		if (msg && msg.replace) msg = msg.replace(/\s+/g, ' ').trim();

		if (!msg || !msg.length) {
			if (node) {
				var nodeName = node.nodeName || 'unknown';

				
				
				
				if (nodeName === 'VIDEO') {
					return;
				}

				msg = `error event from node of ${nodeName}, no message provided?`;
			} else {
				msg = 'error event fired, no relevant message or node found';
			}
		}

		var logPrefix = 'ERROR caught in js/inline/register_global_error_handler';

		msg = `${logPrefix} - ${msg}`;

		genericLogger.error(msg);
	}

	
	
	
	var capture = true;

	
	var throttledHandler = throttle(globalErrorHandler, 10000);

	window.addEventListener('error', throttledHandler, capture);
})();
</script><script type="text/javascript" crossorigin="anonymous" src="https://a.slack-edge.com/bv1-13/manifest.86d0b3ff3d359e0568b7.primer.min.js" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null"></script><noscript><meta http-equiv="refresh" content="0; URL=/?redir=%2Ffiles-pri%2FT0696NU0K9D-F08SY4PBUK1%2Fdownload%2Fazure_cost_ps1.txt&amp;nojsmode=1"></noscript><script type="text/javascript">var safe_hosts = ['app.optimizely.com', 'tinyspeck.dev.slack.com'];

if (self !== top && safe_hosts.indexOf(top.location.host) === -1) {
	window.document.write(
		'\u003Cstyle>body * {display:none !important;}\u003C/style>\u003Ca href="#" onclick=' +
			'"top.location.href=window.location.href" style="display:block !important;padding:10px">Go to Slack.com\u003C/a>'
	);
}

(function() {
	var timer;
	if (self !== top && safe_hosts.indexOf(top.location.host) === -1) {
		timer = window.setInterval(function() {
			if (window) {
				try {
					var pageEl = document.getElementById('page');
					var clientEl = document.getElementById('client-ui');
					var sectionEls = document.querySelectorAll('nav, header, section');

					pageEl.parentNode.removeChild(pageEl);
					clientEl.parentNode.removeChild(clientEl);
					for (var i = 0; i < sectionEls.length; i++) {
						sectionEls[i].parentNode.removeChild(sectionEls[i]);
					}
					window.TS = null;
					window.TD = null;
					window.clearInterval(timer);
				} catch (e) {}
			}
		}, 200);
	}
})();</script><meta name="facebook-domain-verification" content="chiwsajpoybn2cnqyj9w8mvrey56m0"><script type="text/javascript">
window.dataLayer = window.dataLayer || [];
function gtag(){window.dataLayer.push(arguments);}

gtag('consent', "default", {"ad_storage":"denied","ad_user_data":"denied","ad_personalization":"denied","personalization_storage":"denied","analytics_storage":"denied","functionality_storage":"denied","security_storage":"denied","wait_for_update":1000});

function loadGTM() {
	window.dataLayer.push({
		'gtm.start': Date.now(),
		'event': 'gtm.js',
		'AnalyticsActiveGroups': ",1,2,",
		'policy_ga_only': true,
	});
	var firstScript = document.getElementsByTagName('script')[0];
	var thisScript = document.createElement('script');
	thisScript.async = true;
	thisScript.src = '//www.googletagmanager.com/gtm.js?id=GTM-KH2LPK';
	firstScript.parentNode.insertBefore(thisScript, firstScript);
}


</script><script type="text/javascript">
document.addEventListener("DOMContentLoaded", function(e) {
	var gtmDataLayer = window.dataLayer || [];
	var gtmTags = document.querySelectorAll('*[data-gtm-click]');
	var gtmClickHandler = function(c) {
		var gtm_events = this.getAttribute('data-gtm-click');
		if (!gtm_events) return;
		var gtm_events_arr = gtm_events.split(",");
		for(var e=0; e < gtm_events_arr.length; e++) {
			var ev = gtm_events_arr[e].trim();
			gtmDataLayer.push({ 'event': ev });
		}
	};
	for(var g=0; g < gtmTags.length; g++){
		var elem = gtmTags[g];
		elem.addEventListener('click', gtmClickHandler);
	}
});
</script><script type="text/javascript">
(function(e,c,b,f,d,g,a){e.SlackBeaconObject=d;
e[d]=e[d]||function(){(e[d].q=e[d].q||[]).push([1*new Date(),arguments])};
e[d].l=1*new Date();g=c.createElement(b);a=c.getElementsByTagName(b)[0];
g.async=1;g.src=f;a.parentNode.insertBefore(g,a)
})(window,document,"script","https://a.slack-edge.com/bv1-13/slack_beacon.1f9ab05446fdf309c62d.min.js","sb");
window.sb('set', 'token', '3307f436963e02d4f9eb85ce5159744c');
window.sb('track', 'pageview');
</script><script src="https://cdn.cookielaw.org/scripttemplates/otSDKStub.js" data-document-language="true" data-domain-script="3bcd90cf-1e32-46d7-adbd-634f66b65b7d"></script><script>window.OneTrustLoaded = true;</script><script>
window.dataLayer = window.dataLayer || [];

function afterConsentScripts() {
	window.TD.analytics.doPush();

	const bottomBannerEl = document.querySelector('.c-announcement-banner-bottom');
	if (bottomBannerEl !== null) {
		bottomBannerEl.classList.remove('c-announcement-banner-bottom-invisible');
	}
}



var initOneTrustReady = false;
function OptanonWrapper() {
	gtag('consent', "update", {"ad_storage":"denied","ad_user_data":"denied","ad_personalization":"denied","personalization_storage":"denied","analytics_storage":"denied","functionality_storage":"denied","security_storage":"denied"});
	window.dataLayer.push({'event': 'OneTrustReady'});
	if (!initOneTrustReady) {
		document.dispatchEvent(new CustomEvent('OneTrustReady'));
		loadGTM();
		initOneTrustReady = true;
	}

	if (!Optanon.GetDomainData().ShowAlertNotice || false) {
		afterConsentScripts();
	} else {
		document.querySelector('#onetrust-accept-btn-handler').focus()
	}
	Optanon.OnConsentChanged(function() {
		afterConsentScripts();
	});

}</script><script type="text/javascript">var TS_last_log_date = null;
var TSMakeLogDate = function() {
	var date = new Date();

	var y = date.getFullYear();
	var mo = date.getMonth()+1;
	var d = date.getDate();

	var time = {
	  h: date.getHours(),
	  mi: date.getMinutes(),
	  s: date.getSeconds(),
	  ms: date.getMilliseconds()
	};

	Object.keys(time).map(function(moment, index) {
		if (moment == 'ms') {
			if (time[moment] < 10) {
				time[moment] = time[moment]+'00';
			} else if (time[moment] < 100) {
				time[moment] = time[moment]+'0';
			}
		} else if (time[moment] < 10) {
			time[moment] = '0' + time[moment];
		}
	});

	var str = y + '/' + mo + '/' + d + ' ' + time.h + ':' + time.mi + ':' + time.s + '.' + time.ms;
	if (TS_last_log_date) {
		var diff = date-TS_last_log_date;
		//str+= ' ('+diff+'ms)';
	}
	TS_last_log_date = date;
	return str+' ';
}

var parseDeepLinkRequest = function(code) {
	var m = code.match(/"id":"([CDG][A-Z0-9]{8,})"/);
	var id = m ? m[1] : null;

	m = code.match(/"team":"(T[A-Z0-9]{8,})"/);
	var team = m ? m[1] : null;

	m = code.match(/"message":"([0-9]+\.[0-9]+)"/);
	var message = m ? m[1] : null;

	return { id: id, team: team, message: message };
}

if ('rendererEvalAsync' in window) {
	var origRendererEvalAsync = window.rendererEvalAsync;
	window.rendererEvalAsync = function(blob) {
		try {
			var data = JSON.parse(decodeURIComponent(atob(blob)));
			if (data.code.match(/handleDeepLink/)) {
				var request = parseDeepLinkRequest(data.code);
				if (!request.id || !request.team || !request.message) return;

				request.cmd = 'channel';
				TSSSB.handleDeepLinkWithArgs(JSON.stringify(request));
				return;
			} else {
				origRendererEvalAsync(blob);
			}
		} catch (e) {
		}
	}
}</script><script type="text/javascript">var TSSSB = {
	call: function() {
		return false;
	}
};</script><title>Sign in to MediaRadar | Slack</title><meta name="referrer" content="no-referrer"><meta name="superfish" content="nofish"><meta name="author" content="Slack"><meta name="description" content="Find and sign in to your Slack workspace."><meta name="keywords" content=""><meta name="HandheldFriendly" content="true"><meta name="viewport" content="width=device-width, initial-scale=1.0"><meta name="robots" content="noindex, nofollow"></head><body class="full_height"><main id="main"><div id="page_contents"><div id="props_node" data-props="{&quot;recaptchaSitekey&quot;:&quot;6LcQQiYUAAAAADxJHrihACqD5wf3lksm9jbnRY5k&quot;,&quot;crumbValue&quot;:&quot;s-1747818463-831cb611f51da79975e95a872a1ccd671c34a8b4ba6e03b6b1d4caef97cd0ee1-\u2603&quot;,&quot;isSSB&quot;:false,&quot;isSSBBrowserSignin&quot;:false,&quot;isSSBSonicBrowserSignin&quot;:false,&quot;redirectURL&quot;:&quot;%2Ffiles-pri%2FT0696NU0K9D-F08SY4PBUK1%2Fdownload%2Fazure_cost_ps1.txt&quot;,&quot;noSSO&quot;:false,&quot;isSSORequiredForAll&quot;:false,&quot;isSSOOptional&quot;:false,&quot;isSSORequiredForOwners&quot;:false,&quot;isTrailheadPlayground&quot;:false,&quot;forceEmailPasswordLogin&quot;:false,&quot;samlProviderLabel&quot;:&quot;SAML&quot;,&quot;samlLogooutMayBeRequired&quot;:false,&quot;idpConfigDetails&quot;:{&quot;3fe16d7d59ceb256a724d185e28e99a18be842f9ef8a60f45b8e7d72c99b7e9c&quot;:&quot;Google&quot;},&quot;ssoAction&quot;:&quot;login&quot;,&quot;logo&quot;:{},&quot;orgUrl&quot;:&quot;https:\/\/mediaradar.enterprise.slack.com\/&quot;,&quot;enterprise&quot;:{&quot;name&quot;:&quot;MediaRadar&quot;,&quot;domain&quot;:&quot;mediaradar&quot;},&quot;SSBVersion&quot;:&quot;&quot;,&quot;deeplinkSchemes&quot;:{&quot;slack&quot;:&quot;SLACK&quot;,&quot;slackmdm&quot;:&quot;SLACK_MDM&quot;,&quot;slackintune&quot;:&quot;SLACK_INTUNE&quot;,&quot;slackdebug&quot;:&quot;SLACK_DEBUG&quot;,&quot;slackbeta&quot;:&quot;SLACK_BETA&quot;,&quot;slackprototype&quot;:&quot;SLACK_PROTOTYPE&quot;,&quot;voyager&quot;:&quot;VOYAGER&quot;,&quot;slackdeveloper&quot;:&quot;SLACK_DEVELOPER&quot;},&quot;errors&quot;:{&quot;isNoMatch&quot;:false,&quot;isRateLimited&quot;:false,&quot;isMissing&quot;:false,&quot;isIncorrectUser&quot;:false,&quot;isDeleted&quot;:false,&quot;isIncorrectPassword&quot;:false,&quot;isEmailAssociatedWithDeactivatedAccount&quot;:false,&quot;isSSOIncorrectlyFormatted&quot;:false,&quot;isTwoFactorWrong&quot;:false,&quot;isSMSRateLimited&quot;:false,&quot;isIncorrectTwoFactorState&quot;:false,&quot;isNoSSOOwner&quot;:false,&quot;isNoSSORa&quot;:false,&quot;hasSSORequiredForOwnersError&quot;:false},&quot;isIOS&quot;:false}"></div></div></main><script type="text/javascript">
/**
 * A placeholder function that the build script uses to
 * replace file paths with their CDN versions.
 *
 * @param {String} file_path - File path
 * @returns {String}
 */
function vvv(file_path) {
		 var vvv_warning = 'You cannot use vvv on dynamic values. Please make sure you only pass in static file paths.'; if (window.TS && window.TS.warn) { window.TS.warn(vvv_warning); } else { console.warn(vvv_warning); } 
	return file_path;
}

var cdn_url = "https:\/\/a.slack-edge.com";
var vvv_abs_url = "https:\/\/slack.com\/";
var inc_js_setup_data = {
	emoji_sheets: {
		apple: 'https://a.slack-edge.com/80588/img/emoji_2017_12_06/sheet_apple_64_indexed_256.png',
		google: 'https://a.slack-edge.com/80588/img/emoji_2017_12_06/sheet_google_64_indexed_256.png',
	},
};
</script><script nonce="" type="text/javascript">	// common boot_data
	var boot_data = {"cdn":{"edges":["https:\/\/a.slack-edge.com\/","https:\/\/b.slack-edge.com\/","https:\/\/a.slack-edge.com\/"],"avatars":"https:\/\/ca.slack-edge.com\/","downloads":"https:\/\/downloads.slack-edge.com\/","files":"https:\/\/slack-files.com\/"},"feature_builder_story_step":false,"feature_olug_remove_required_workspace_setting":false,"feature_file_threads":true,"feature_broadcast_indicator":true,"feature_sonic_emoji":true,"feature_attachments_inline":false,"feature_desktop_symptom_events":false,"feature_gdpr_user_join_tos":true,"feature_user_invite_tos_april_2018":true,"feature_channel_mgmt_message_count":false,"feature_channel_exports":false,"feature_allow_intra_word_formatting":true,"feature_slim_scrollbar":false,"feature_edge_upload_proxy_check":false,"feature_set_tz_automatically":true,"feature_attachments_v2":true,"feature_beacon_js_errors":false,"feature_user_app_disable_speed_bump":false,"feature_apps_manage_permissions_scope_changes":true,"feature_ia_member_profile":true,"feature_desktop_reload_on_generic_error":true,"feature_desktop_extend_app_menu":true,"feature_desktop_restart_service_worker":false,"feature_wta_stop_creation":true,"feature_admin_email_change_confirm":false,"feature_improved_email_rendering":true,"feature_recent_desktop_files":true,"feature_cea_allowlist_changes":false,"feature_cea_channel_management":true,"feature_cea_admin_controls":true,"feature_cea_allowlist_changes_plus":true,"feature_ia_layout":true,"feature_threaded_call_block":true,"feature_enterprise_mobile_device_check":true,"feature_trace_jq_init":true,"feature_seven_days_email_update":true,"feature_channel_sections":true,"feature_show_email_forwarded_by":false,"feature_mpdm_audience_expansion":true,"feature_remove_email_preview_link":true,"feature_desktop_enable_tslog":false,"feature_email_determine_charset":true,"feature_no_deprecation_in_updater":false,"feature_pea_domain_allowlist":true,"feature_composer_auth_admin":false,"experiment_assignments":{"innovations_lp":{"experiment_id":"8253871971107","type":"visitor","group":"on","schedule_ts":1746643793,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"enterprise_search_lp":{"experiment_id":"8787098737047","type":"visitor","group":"on","schedule_ts":1746641312,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"signin_design_refresh":{"experiment_id":"8656625636818","type":"visitor","group":"on","trigger":"hash_visitor","schedule_ts":1746462640,"log_exposures":true,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"pro_discount_btf":{"experiment_id":"8830623175587","type":"visitor","group":"treatment","trigger":"hash_visitor","schedule_ts":1746747238,"log_exposures":true,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"aswebauth_cookie_session":{"experiment_id":"7920012625699","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"sandbox_password_free_signin":{"experiment_id":"8766535145014","type":"visitor","group":"on","schedule_ts":1746821450,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"spam_email_recaptcha":{"experiment_id":"8449964217459","type":"visitor","group":"on","schedule_ts":1746141854,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"limit_confirmation_code_for_alias_email":{"experiment_id":"8695671490005","type":"visitor","group":"on","schedule_ts":1744822321,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"modular_pages_webapp":{"experiment_id":"8510095101942","type":"visitor","group":"on","schedule_ts":1745544497,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"april_pages2":{"experiment_id":"8743286716099","type":"visitor","group":"on","schedule_ts":1745440781,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"mris_extension":{"experiment_id":"4746206947365","type":"visitor","group":"on","schedule_ts":1706216371,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"on24_extension":{"experiment_id":"4772019824211","type":"visitor","group":"on","schedule_ts":1675891595,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"solution_gallery":{"experiment_id":"7805433315095","type":"visitor","group":"on","schedule_ts":1730293827,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"homepage_hero_atf_copy":{"experiment_id":"8709043044887","type":"visitor","group":"control","trigger":"hash_visitor","schedule_ts":1744824020,"log_exposures":true,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"miro_marketplace":{"experiment_id":"8704952792931","type":"visitor","group":"on","schedule_ts":1745012384,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"ai_paid_campaign":{"experiment_id":"7385757537713","type":"visitor","group":"on","schedule_ts":1721664156,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"cust_acq_slack_ai_apr_17":{"experiment_id":"6946248676101","type":"visitor","group":"on","schedule_ts":1713441387,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"sticky_fyp_cta":{"experiment_id":"8281275470644","type":"visitor","group":"control","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"downloads_launch":{"experiment_id":"8552687986532","type":"visitor","group":"on","schedule_ts":1741711036,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"add_team_creation_flow_segmentation":{"experiment_id":"8399073017011","type":"visitor","group":"on","schedule_ts":1741620581,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"hp_mobile_revamp_fy26":{"experiment_id":"8427344932433","type":"visitor","group":"on","schedule_ts":1741262676,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"trials_for_new_teams":{"experiment_id":"8367344188583","type":"visitor","group":"variation_a","trigger":"hash_visitor","schedule_ts":1740168641,"log_exposures":true,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"oauth_domain_signin_fix":{"experiment_id":"7214626556485","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"activation_enterprise_signin_primer":{"experiment_id":"6443324713893","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"sf_fonts_latin":{"experiment_id":"7577927212402","type":"visitor","group":"on","schedule_ts":1739573433,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"sf_fonts_cjk":{"experiment_id":"7575044373701","type":"visitor","group":"on","schedule_ts":1739573413,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"plan_features_unification":{"experiment_id":"7756491172532","type":"visitor","group":"on","schedule_ts":1737129742,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"slack_developer_program_trailhead_optimization":{"experiment_id":"8018607835799","type":"visitor","group":"on","schedule_ts":1737049638,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketplace_add2":{"experiment_id":"8251117941749","type":"visitor","group":"on","schedule_ts":1736280665,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"screen_text_2fa":{"experiment_id":"7846147603012","type":"visitor","group":"on","schedule_ts":1734375504,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"app_directory_connectors_collection":{"experiment_id":"6321714753558","type":"visitor","group":"on","schedule_ts":1705448247,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"app_directory_connectors":{"experiment_id":"6144504493874","type":"visitor","group":"treatment","schedule_ts":1705354312,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"app_directory_coral":{"experiment_id":"8121125935588","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"marketplace_add":{"experiment_id":"7940445156581","type":"visitor","group":"on","schedule_ts":1732060351,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"limit_confirmation_code_for_email":{"experiment_id":"7790568146529","type":"visitor","group":"on","schedule_ts":1727471403,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"slack_developer_program_trailhead_integration":{"experiment_id":"7459994387075","type":"visitor","group":"on","schedule_ts":1730926934,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"demo_refresh":{"experiment_id":"7857496818678","type":"visitor","group":"on","schedule_ts":1730319250,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"cust_acq_homepage_cta_v2":{"experiment_id":"7463935352501","type":"visitor","group":"treatment","schedule_ts":1721761280,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"api_docs_simplify_tutorials":{"experiment_id":"7629258500165","type":"visitor","group":"on","schedule_ts":1729024584,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"contact_sales_dept_removal":{"experiment_id":"6538486873169","type":"visitor","group":"treatment","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"salesforce_slack_integration":{"experiment_id":"7618340570659","type":"visitor","group":"on","schedule_ts":1727389057,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketing_ad_app_store_urls":{"experiment_id":"7746105288676","type":"visitor","group":"on","schedule_ts":1726699830,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"search_zd_vs_solr":{"experiment_id":"1355709002145","type":"visitor","group":"control","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"desktop_updater_v3_public_latest_release":{"experiment_id":"6724457596097","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"slack_developer_join_settings_rearch":{"experiment_id":"6917822477282","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"phone_number_ie":{"experiment_id":"7441623906036","type":"visitor","group":"treatment","schedule_ts":1721414251,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_ko":{"experiment_id":"7276620382240","type":"visitor","group":"treatment","schedule_ts":1719852648,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_pt":{"experiment_id":"7276731000896","type":"visitor","group":"treatment","schedule_ts":1719253468,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_es":{"experiment_id":"7266548128369","type":"visitor","group":"treatment","schedule_ts":1719253482,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_it":{"experiment_id":"7250980705925","type":"visitor","group":"treatment","schedule_ts":1719253456,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_br":{"experiment_id":"7256414077620","type":"visitor","group":"treatment","schedule_ts":1719244830,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"slack_developer_program":{"experiment_id":"5782848233798","type":"visitor","group":"on","schedule_ts":1709242483,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_ca":{"experiment_id":"7256320861108","type":"visitor","group":"treatment","schedule_ts":1718813429,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"downloads_s2p_promo":{"experiment_id":"7132023966439","type":"visitor","group":"control","trigger":"hash_visitor","schedule_ts":1718307458,"log_exposures":true,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"phone_number_au":{"experiment_id":"7168837725445","type":"visitor","group":"treatment","schedule_ts":1716912663,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_fr":{"experiment_id":"7157169096183","type":"visitor","group":"treatment","schedule_ts":1716912646,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_de":{"experiment_id":"7168806134229","type":"visitor","group":"treatment","schedule_ts":1716908444,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"phone_number_jp":{"experiment_id":"7174178940996","type":"visitor","group":"treatment","schedule_ts":1716583378,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketing_sc_rum_integration":{"experiment_id":"2491795733043","type":"visitor","group":"off","trigger":"hash_visitor","schedule_ts":1631738284,"log_exposures":true,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"paid_lp_expand":{"experiment_id":"7134287733637","type":"visitor","group":"treatment","schedule_ts":1717713269,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketing_live_chat_emea":{"experiment_id":"7226533858036","type":"visitor","group":"on","schedule_ts":1717629445,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"crypto_sidecar_for_comparable_keychains":{"experiment_id":"7041197731428","type":"visitor","group":"on","schedule_ts":1715184970,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"new_paid_lp":{"experiment_id":"6818768684695","type":"visitor","group":"treatment","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"slack_elevate_launch":{"experiment_id":"6966627699558","type":"visitor","group":"on","schedule_ts":1713959798,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketing_recaptcha_hc":{"experiment_id":"6963734115829","type":"visitor","group":"on","schedule_ts":1713301140,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketing_hc_flow_specifier":{"experiment_id":"6989238991504","type":"visitor","group":"on","schedule_ts":1713296815,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"anthony_test_visitor_1":{"experiment_id":"6823470010164","type":"visitor","group":"treatment","schedule_ts":1712613615,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"slack_developer_program_tos":{"experiment_id":"6725223824534","type":"visitor","group":"on","schedule_ts":1710376976,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketing_media_kit":{"experiment_id":"6696687337684","type":"visitor","group":"on","schedule_ts":1709232747,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"mris_usercreator_live":{"experiment_id":"4921780175444","type":"visitor","group":"on","schedule_ts":1706216435,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"out_of_office_xmas_jp":{"experiment_id":"6296845198293","type":"visitor","group":"off","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"out_of_office_xmas":{"experiment_id":"6322553087328","type":"visitor","group":"off","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"marketing_hreflang_errors_fix":{"experiment_id":"6319747700807","type":"visitor","group":"on","schedule_ts":1702931766,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"eg_pricing":{"experiment_id":"6266727458225","type":"visitor","group":"on","schedule_ts":1702587412,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"new_gated_demo":{"experiment_id":"6171698537921","type":"visitor","group":"on","schedule_ts":1701209093,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"ia4_t3_asset_refresh":{"experiment_id":"6177775777761","type":"visitor","group":"on","schedule_ts":1700246503,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"marketing_cj":{"experiment_id":"5820701519667","type":"visitor","group":"on","schedule_ts":1699033035,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"deny_russian_ip":{"experiment_id":"3201051153989","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"swap_ukraine_logo_toggle":{"experiment_id":"5598910456034","type":"visitor","group":"on","schedule_ts":1689885040,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"customer_awards_launch":{"experiment_id":"2673548411155","type":"visitor","group":"on","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"},"slack_ie_address":{"experiment_id":"4857849748754","type":"visitor","group":"on","schedule_ts":1677793396,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"proj_brand_customer_stories_lp":{"experiment_id":"3448021380448","type":"visitor","group":"on","schedule_ts":1653596127,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"digital_first_lightning_strike_custacq":{"experiment_id":"2220660679364","type":"visitor","group":"on","schedule_ts":1625075563,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"cust_acq_partners_template":{"experiment_id":"2232204551504","type":"visitor","group":"treatment","schedule_ts":1628191410,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"community_launch":{"experiment_id":"2652841576373","type":"visitor","group":"on","schedule_ts":1635871147,"exposure_id":"060e09b6f483b13d9433fc05eb563485","trigger":"launch_visitor","log_exposures":false},"app_directory_aws_promotion_banner":{"experiment_id":"3025397781073","type":"visitor","group":"control","trigger":"finished","log_exposures":false,"exposure_id":"060e09b6f483b13d9433fc05eb563485"}},"no_login":false};</script><script type="text/javascript" crossorigin="anonymous" src="https://a.slack-edge.com/bv1-13/primer-vendor.8053ef7b3d5d7fad6d8b.primer.min.js" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null"></script><script type="text/javascript" crossorigin="anonymous" src="https://a.slack-edge.com/bv1-13/enterprise-signin-core.28471eadce3d2a8049ff.primer.min.js" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null"></script><link href="https://a.slack-edge.com/bv1-13/enterprise-signin-core.dfe440c854a108754331.primer.min.css" rel="stylesheet" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous"><link href="https://a.slack-edge.com/67327e9/style/rollup-slack_kit_base.css" rel="stylesheet" id="slack_kit_helpers" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous"><link href="https://a.slack-edge.com/824ebf6/style/rollup-slack_kit_helpers.css" rel="stylesheet" id="slack_kit_helpers" type="text/css" onload="window._cdn ? _cdn.ok(this, arguments) : null" onerror="window._cdn ? _cdn.failed(this, arguments) : null" crossorigin="anonymous">

<!-- slack-www-hhvm-main-iad-sdht/ 2025-05-21 02:07:43/ vf9f3e887eb442010cacab3b82a51bbb527231061/ B:H -->

</body></html>