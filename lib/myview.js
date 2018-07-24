"use babel";

//TEST PAGE
export default class Myview {

    constructor(filePath) {
        this.uri = filePath
        this.element = document.createElement('div');
        this.element.classList.add('your-name-word-count');

        // Create message element
        const message = document.createElement('div');
        message.textContent = 'The YourNameWordCount package is Alive! It\'s ALIVE!';
        message.classList.add('message');
        this.element.appendChild(message);
    }

    serialize() {}

    destroy() {
        this.url.remove()
        this.element.remove()
    }

    getURI() {
        return this.uri
    }

    getTitle() {
        return 'Myviewxx'
    }

    getIconName() {
        return 'dashboard'
    }

}
