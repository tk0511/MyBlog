'use strict';

function switchContents(article) {
    if (article.style.maxHeight !== '100%') {
        article.style.maxHeight = '100%';
        article.className += ' article-contents-actived'
    }
}